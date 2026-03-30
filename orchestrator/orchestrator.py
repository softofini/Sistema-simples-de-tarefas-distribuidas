"""
Orquestrador Principal (Coordenador)
=====================================
Responsabilidades:
- Receber tarefas de clientes autenticados via TCP
- Distribuir tarefas para workers usando política Round Robin
- Manter ciclo de vida completo das tarefas (PENDING/ASSIGNED/RUNNING/COMPLETED/FAILED)
- Reatribuir apenas tarefas interrompidas
- Monitorar workers via heartbeat com tolerância a falso positivo
- Sincronizar estado global completo com o backup via UDP Multicast
"""

import socket
import threading
import time
import uuid
import struct
import sys
import os
from typing import Dict, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.lamport_clock import LamportClock
from utils.protocol import Message, MessageType, Task, TaskStatus, AuthManager
from utils.logger import setup_logger, log_event


class Orchestrator:
    def __init__(
        self,
        host: str = '0.0.0.0',
        client_port: int = 5000,
        worker_port: int = 5001,
        multicast_group: str = '224.1.1.1',
        multicast_port: int = 5007,
        node_id: str = 'orchestrator_primary',
        role: str = 'primary',
    ):
        # Configurações de rede
        self.host = host
        self.client_port = client_port
        self.worker_port = worker_port
        self.multicast_group = multicast_group
        self.multicast_port = multicast_port

        # Identidade do nó
        self.node_id = node_id
        self.role = role

        # Estado do sistema
        self.tasks: Dict[str, Task] = {}
        self.workers: Dict[str, dict] = {}
        self.rr_index = 0
        self.max_task_retries = 3

        # Runtime
        self.is_primary = True
        self.is_running = True

        # Componentes
        self.clock = LamportClock(self.node_id)
        self.auth_manager = AuthManager(token_ttl_seconds=3600)
        self.logger = setup_logger(self.node_id)

        # Locks
        self._tasks_lock = threading.Lock()
        self._workers_lock = threading.Lock()

        # Estado runtime não replicável
        self._worker_connections: Dict[str, socket.socket] = {}
        self._worker_send_locks: Dict[str, threading.Lock] = {}

        log_event(
            self.logger,
            self.clock.tick(),
            "INIT",
            f"{self.node_id} inicializado em {host}:{client_port}/{worker_port}",
        )

    def start(self):
        """Inicia todos os serviços do orquestrador."""
        log_event(self.logger, self.clock.tick(), "START", "Iniciando serviços do orquestrador")

        client_thread = threading.Thread(target=self._accept_clients, daemon=True)
        client_thread.start()

        worker_thread = threading.Thread(target=self._accept_workers, daemon=True)
        worker_thread.start()

        heartbeat_thread = threading.Thread(target=self._heartbeat_monitor, daemon=True)
        heartbeat_thread.start()

        sync_thread = threading.Thread(target=self._multicast_sync, daemon=True)
        sync_thread.start()

        orchestrator_heartbeat_thread = threading.Thread(target=self._send_orchestrator_heartbeats, daemon=True)
        orchestrator_heartbeat_thread.start()

        log_event(self.logger, self.clock.tick(), "RUNNING", "Todos os serviços do orquestrador estão ativos")

        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        log_event(self.logger, self.clock.tick(), "SHUTDOWN", "Encerrando orquestrador")
        self.is_running = False

    # ==================== CLIENTES ====================

    def _accept_clients(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.client_port))
        server.listen(20)
        server.settimeout(2)

        log_event(
            self.logger,
            self.clock.tick(),
            "CLIENT_SERVER",
            f"Servidor de clientes ouvindo em {self.host}:{self.client_port}",
        )

        while self.is_running:
            try:
                conn, addr = server.accept()
                log_event(self.logger, self.clock.tick(), "CLIENT_CONNECT", f"Nova conexão de cliente: {addr}")
                handler = threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True)
                handler.start()
            except socket.timeout:
                continue
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "ERROR", f"Erro ao aceitar cliente: {e}")

        server.close()

    def _handle_client(self, conn: socket.socket, addr):
        try:
            while self.is_running:
                data = self._recv_message(conn)
                if not data:
                    break

                msg = Message.from_json(data)
                self.clock.receive_event(msg.lamport_time)
                self._log_receive(msg, f"client:{addr}")

                if msg.msg_type == MessageType.AUTH_REQUEST.value:
                    self._handle_auth(conn, msg, addr)
                elif msg.msg_type == MessageType.TASK_SUBMIT.value:
                    self._handle_task_submit(conn, msg, addr)
                elif msg.msg_type == MessageType.TASK_STATUS_REQUEST.value:
                    self._handle_task_status(conn, msg, addr)
                else:
                    log_event(
                        self.logger,
                        self.clock.tick(),
                        "UNKNOWN_MSG",
                        f"Mensagem desconhecida de {addr}: {msg.msg_type}",
                    )
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "CLIENT_ERROR", f"Erro com cliente {addr}: {e}")
        finally:
            conn.close()
            log_event(self.logger, self.clock.tick(), "CLIENT_DISCONNECT", f"Cliente desconectado: {addr}")

    def _handle_auth(self, conn: socket.socket, msg: Message, addr):
        username = msg.payload.get("username", "")
        password = msg.payload.get("password", "")
        token = self.auth_manager.authenticate(username, password)

        if token:
            response = Message(
                msg_type=MessageType.AUTH_RESPONSE.value,
                sender_id=self.node_id,
                payload={
                    "success": True,
                    "token": token,
                    "message": "Autenticação bem-sucedida",
                    "active_client_endpoint": {"host": self.host, "port": self.client_port},
                    "active_worker_endpoint": {"host": self.host, "port": self.worker_port},
                },
                lamport_time=self.clock.send_event(),
            )
            log_event(
                self.logger,
                self.clock.get_time(),
                "AUTH_SUCCESS",
                f"Usuário '{username}' autenticado de {addr}",
            )
            self._sync_state_to_backup()
        else:
            response = Message(
                msg_type=MessageType.AUTH_RESPONSE.value,
                sender_id=self.node_id,
                payload={"success": False, "token": None, "message": "Credenciais inválidas"},
                lamport_time=self.clock.send_event(),
            )
            log_event(
                self.logger,
                self.clock.get_time(),
                "AUTH_FAILED",
                f"Falha na autenticação do usuário '{username}' de {addr}",
            )

        self._send_message(conn, response.to_json())
        self._log_send(response, f"client:{addr}")

    def _handle_task_submit(self, conn: socket.socket, msg: Message, addr):
        token = msg.token
        username = self.auth_manager.validate_token(token) if token else None

        if not username:
            response = Message(
                msg_type=MessageType.TASK_SUBMIT_ACK.value,
                sender_id=self.node_id,
                payload={"success": False, "message": "Token inválido. Autentique-se primeiro."},
                lamport_time=self.clock.send_event(),
            )
            self._send_message(conn, response.to_json())
            self._log_send(response, f"client:{addr}")
            log_event(self.logger, self.clock.get_time(), "TASK_REJECTED", f"Tarefa rejeitada de {addr}: token inválido")
            return

        now = time.time()
        task_id = str(uuid.uuid4())[:8]
        task = Task(
            task_id=task_id,
            client_id=username,
            description=msg.payload.get("description", "Tarefa sem descrição"),
            status=TaskStatus.PENDING.value,
            created_at=now,
            lamport_created=self.clock.get_time(),
            last_updated_lamport=self.clock.get_time(),
        )

        with self._tasks_lock:
            self.tasks[task_id] = task

        log_event(
            self.logger,
            self.clock.tick(),
            "TASK_SUBMITTED",
            f"Tarefa '{task_id}' submetida por '{username}' | Descrição: {task.description}",
        )

        assigned = self._distribute_task(task)

        response = Message(
            msg_type=MessageType.TASK_SUBMIT_ACK.value,
            sender_id=self.node_id,
            payload={
                "success": True,
                "task_id": task_id,
                "status": task.status,
                "message": f"Tarefa '{task_id}' recebida e {'distribuída' if assigned else 'aguardando worker'}",
            },
            lamport_time=self.clock.send_event(),
        )
        self._send_message(conn, response.to_json())
        self._log_send(response, f"client:{addr}")
        self._sync_state_to_backup()

    def _handle_task_status(self, conn: socket.socket, msg: Message, addr):
        token = msg.token
        username = self.auth_manager.validate_token(token) if token else None

        if not username:
            response = Message(
                msg_type=MessageType.TASK_STATUS_RESPONSE.value,
                sender_id=self.node_id,
                payload={"success": False, "message": "Token inválido"},
                lamport_time=self.clock.send_event(),
            )
            self._send_message(conn, response.to_json())
            self._log_send(response, f"client:{addr}")
            return

        task_id = msg.payload.get("task_id")
        with self._tasks_lock:
            if task_id:
                task = self.tasks.get(task_id)
                if task and task.client_id == username:
                    tasks_info = [task.to_dict()]
                else:
                    tasks_info = []
            else:
                tasks_info = [t.to_dict() for t in self.tasks.values() if t.client_id == username]

        response = Message(
            msg_type=MessageType.TASK_STATUS_RESPONSE.value,
            sender_id=self.node_id,
            payload={"success": True, "tasks": tasks_info},
            lamport_time=self.clock.send_event(),
        )
        self._send_message(conn, response.to_json())
        self._log_send(response, f"client:{addr}")

        log_event(self.logger, self.clock.get_time(), "TASK_STATUS_QUERY", f"Consulta de status por '{username}' de {addr}")

    # ==================== WORKERS ====================

    def _accept_workers(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.worker_port))
        server.listen(20)
        server.settimeout(2)

        log_event(
            self.logger,
            self.clock.tick(),
            "WORKER_SERVER",
            f"Servidor de workers ouvindo em {self.host}:{self.worker_port}",
        )

        while self.is_running:
            try:
                conn, addr = server.accept()
                log_event(self.logger, self.clock.tick(), "WORKER_CONNECT", f"Nova conexão de worker: {addr}")
                handler = threading.Thread(target=self._handle_worker, args=(conn, addr), daemon=True)
                handler.start()
            except socket.timeout:
                continue
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "ERROR", f"Erro ao aceitar worker: {e}")

        server.close()

    def _handle_worker(self, conn: socket.socket, addr):
        worker_id = None
        try:
            while self.is_running:
                data = self._recv_message(conn)
                if not data:
                    break

                msg = Message.from_json(data)
                self.clock.receive_event(msg.lamport_time)
                self._log_receive(msg, f"worker:{addr}")

                if msg.msg_type == MessageType.WORKER_REGISTER.value:
                    worker_id = msg.sender_id
                    self._register_worker(worker_id, conn, addr, msg)
                    self._distribute_pending_tasks()

                elif msg.msg_type == MessageType.HEARTBEAT.value:
                    self._handle_heartbeat(worker_id)

                elif msg.msg_type == MessageType.TASK_STARTED.value:
                    task_id = msg.payload.get("task_id")
                    self._handle_task_started(worker_id, task_id)

                elif msg.msg_type == MessageType.TASK_COMPLETE.value:
                    task_id = msg.payload.get("task_id")
                    result = msg.payload.get("result", "")
                    self._handle_task_complete(worker_id, task_id, result)

                elif msg.msg_type == MessageType.TASK_FAILED.value:
                    task_id = msg.payload.get("task_id")
                    reason = msg.payload.get("reason", "Falha desconhecida")
                    self._handle_task_failure(worker_id, task_id, reason)

        except Exception as e:
            log_event(self.logger, self.clock.tick(), "WORKER_ERROR", f"Erro com worker '{worker_id}' em {addr}: {e}")
        finally:
            if worker_id:
                self._handle_worker_disconnect(worker_id, reason="socket encerrado")
            try:
                conn.close()
            except Exception:
                pass

    def _register_worker(self, worker_id: str, conn: socket.socket, addr, msg: Message):
        primary_endpoint = msg.payload.get("primary_endpoint")
        secondary_endpoint = msg.payload.get("secondary_endpoint")
        now = time.time()

        with self._workers_lock:
            old_conn = self._worker_connections.get(worker_id)
            if old_conn and old_conn is not conn:
                try:
                    old_conn.close()
                except Exception:
                    pass

            self._worker_connections[worker_id] = conn
            self._worker_send_locks[worker_id] = self._worker_send_locks.get(worker_id, threading.Lock())
            self.workers[worker_id] = {
                "worker_id": worker_id,
                "addr": f"{addr[0]}:{addr[1]}",
                "load": 0,
                "active": True,
                "last_heartbeat": now,
                "missed_heartbeats": 0,
                "registered_at": now,
                "primary_endpoint": primary_endpoint,
                "secondary_endpoint": secondary_endpoint,
                "last_lamport": self.clock.get_time(),
            }

        log_event(
            self.logger,
            self.clock.tick(),
            "WORKER_REGISTERED",
            f"Worker '{worker_id}' registrado de {addr} | primário={primary_endpoint} secundário={secondary_endpoint}",
        )

        response = Message(
            msg_type=MessageType.WORKER_REGISTER_ACK.value,
            sender_id=self.node_id,
            payload={
                "success": True,
                "message": "Worker registrado com sucesso",
                "active_worker_endpoint": {"host": self.host, "port": self.worker_port},
                "active_client_endpoint": {"host": self.host, "port": self.client_port},
            },
            lamport_time=self.clock.send_event(),
        )
        self._send_worker_message(worker_id, conn, response.to_json())
        self._log_send(response, f"worker:{worker_id}")
        self._sync_state_to_backup()

    def _handle_heartbeat(self, worker_id: Optional[str]):
        if not worker_id:
            return

        with self._workers_lock:
            worker = self.workers.get(worker_id)
            conn = self._worker_connections.get(worker_id)
            if not worker or not conn:
                return
            worker["last_heartbeat"] = time.time()
            worker["active"] = True
            worker["missed_heartbeats"] = 0
            worker["last_lamport"] = self.clock.get_time()

        response = Message(
            msg_type=MessageType.HEARTBEAT_ACK.value,
            sender_id=self.node_id,
            payload={},
            lamport_time=self.clock.send_event(),
        )
        self._send_worker_message(worker_id, conn, response.to_json())

    def _handle_task_started(self, worker_id: Optional[str], task_id: Optional[str]):
        if not worker_id or not task_id:
            return

        changed = False
        with self._tasks_lock:
            task = self.tasks.get(task_id)
            if task and task.assigned_worker == worker_id and task.status == TaskStatus.ASSIGNED.value:
                task.status = TaskStatus.RUNNING.value
                task.started_at = time.time()
                task.lamport_started = self.clock.get_time()
                if not task.first_started_worker:
                    task.first_started_worker = worker_id
                # Registrar no histórico de execução
                task.execution_history.append({
                    "worker_id": worker_id,
                    "event": "STARTED",
                    "timestamp": task.started_at,
                    "lamport_time": task.lamport_started,
                })
                task.last_updated_lamport = self.clock.get_time()
                changed = True

        if changed:
            log_event(
                self.logger,
                self.clock.tick(),
                "TASK_RUNNING",
                f"Tarefa '{task_id}' entrou em RUNNING no worker '{worker_id}'",
            )
            self._sync_state_to_backup()

    def _handle_task_complete(self, worker_id: Optional[str], task_id: Optional[str], result: str):
        if not worker_id or not task_id:
            return

        changed = False
        with self._tasks_lock:
            task = self.tasks.get(task_id)
            if task and task.assigned_worker == worker_id and task.status != TaskStatus.COMPLETED.value:
                task.status = TaskStatus.COMPLETED.value
                task.result = result
                task.completed_worker = worker_id
                task.completed_at = time.time()
                task.lamport_completed = self.clock.get_time()
                # Registrar no histórico de execução
                task.execution_history.append({
                    "worker_id": worker_id,
                    "event": "COMPLETED",
                    "timestamp": task.completed_at,
                    "lamport_time": task.lamport_completed,
                })
                task.last_updated_lamport = self.clock.get_time()
                changed = True

        if changed:
            with self._workers_lock:
                worker = self.workers.get(worker_id)
                if worker:
                    worker["load"] = max(0, int(worker.get("load", 0)) - 1)
            log_event(
                self.logger,
                self.clock.tick(),
                "TASK_COMPLETED",
                f"Tarefa '{task_id}' concluída por '{worker_id}'",
            )
            self._sync_state_to_backup()

    def _handle_task_failure(self, worker_id: Optional[str], task_id: Optional[str], reason: str):
        if not worker_id or not task_id:
            return

        reassigned = False
        with self._tasks_lock:
            task = self.tasks.get(task_id)
            if not task:
                return

            is_interrupted = task.assigned_worker == worker_id and task.status in [
                TaskStatus.ASSIGNED.value,
                TaskStatus.RUNNING.value,
            ]
            if not is_interrupted:
                return

            failure_time = time.time()
            task.retries += 1
            task.last_error = reason
            task.failed_workers.append(worker_id)
            task.status = TaskStatus.FAILED.value
            # Registrar no histórico de execução
            task.execution_history.append({
                "worker_id": worker_id,
                "event": "FAILED",
                "timestamp": failure_time,
                "lamport_time": self.clock.get_time(),
                "reason": reason,
            })
            task.last_updated_lamport = self.clock.get_time()

            if task.retries < self.max_task_retries:
                task.status = TaskStatus.PENDING.value
                task.assigned_worker = None
                task.assigned_at = 0.0
                task.started_at = 0.0
                reassigned = True

        with self._workers_lock:
            worker = self.workers.get(worker_id)
            if worker:
                worker["load"] = max(0, int(worker.get("load", 0)) - 1)

        log_event(
            self.logger,
            self.clock.tick(),
            "TASK_FAILED",
            f"Tarefa '{task_id}' falhou no worker '{worker_id}' | tentativa {self.tasks[task_id].retries}/{self.max_task_retries} | motivo={reason}",
        )

        if reassigned:
            log_event(
                self.logger,
                self.clock.tick(),
                "TASK_REASSIGN",
                f"Reatribuindo automaticamente tarefa interrompida '{task_id}' (tentativa {self.tasks[task_id].retries}/{self.max_task_retries})",
            )
            self._distribute_task(self.tasks[task_id])

        self._sync_state_to_backup()

    def _handle_worker_disconnect(self, worker_id: str, reason: str):
        with self._workers_lock:
            worker = self.workers.get(worker_id)
            conn = self._worker_connections.pop(worker_id, None)
            if worker:
                worker["active"] = False
                worker["last_lamport"] = self.clock.get_time()

        if conn:
            try:
                conn.close()
            except Exception:
                pass

        log_event(self.logger, self.clock.tick(), "WORKER_DISCONNECT", f"Worker '{worker_id}' desconectado ({reason})")

        tasks_to_reassign = []
        with self._tasks_lock:
            for task in self.tasks.values():
                if task.assigned_worker != worker_id:
                    continue
                if task.status not in [TaskStatus.ASSIGNED.value, TaskStatus.RUNNING.value]:
                    continue
                if task.status == TaskStatus.COMPLETED.value:
                    continue

                task.retries += 1
                task.last_error = f"worker {worker_id} indisponível"
                task.failed_workers.append(worker_id)
                if task.retries >= self.max_task_retries:
                    task.status = TaskStatus.FAILED.value
                    task.last_updated_lamport = self.clock.get_time()
                else:
                    task.status = TaskStatus.PENDING.value
                    task.assigned_worker = None
                    task.assigned_at = 0.0
                    task.started_at = 0.0
                    task.last_updated_lamport = self.clock.get_time()
                    tasks_to_reassign.append(task)

        for task in tasks_to_reassign:
            log_event(
                self.logger,
                self.clock.tick(),
                "TASK_REASSIGN",
                f"Reatribuindo tarefa interrompida '{task.task_id}' após queda de '{worker_id}'",
            )
            self._distribute_task(task)

        self._sync_state_to_backup()

    # ==================== BALANCEAMENTO ====================

    def _distribute_task(self, task: Task) -> bool:
        with self._workers_lock:
            active_ids = [wid for wid, winfo in self.workers.items() if winfo.get("active")]

        if not active_ids:
            log_event(self.logger, self.clock.tick(), "NO_WORKERS", f"Nenhum worker ativo para tarefa '{task.task_id}'")
            return False

        # Evita reatribuir para workers que já falharam nesta tarefa,
        # desde que exista ao menos um worker alternativo disponível.
        failed_set = set(task.failed_workers or [])
        eligible_ids = [wid for wid in active_ids if wid not in failed_set]
        
        if eligible_ids:
            candidate_ids = eligible_ids
            selection_reason = f"(workers elegíveis, excluindo {failed_set})"
        else:
            candidate_ids = active_ids
            selection_reason = f"(todas as tentativas falharam, retry global)"

        self.rr_index = self.rr_index % len(candidate_ids)
        selected_id = candidate_ids[self.rr_index]
        self.rr_index = (self.rr_index + 1) % len(candidate_ids)

        assign_time = time.time()
        with self._tasks_lock:
            task.status = TaskStatus.ASSIGNED.value
            task.assigned_worker = selected_id
            task.assigned_at = assign_time
            task.lamport_assigned = self.clock.get_time()
            # Registrar atribuição no histórico
            task.execution_history.append({
                "worker_id": selected_id,
                "event": "ASSIGNED",
                "timestamp": assign_time,
                "lamport_time": task.lamport_assigned,
            })
            task.last_updated_lamport = self.clock.get_time()

        with self._workers_lock:
            selected_worker = self.workers.get(selected_id)
            conn = self._worker_connections.get(selected_id)
            if selected_worker:
                selected_worker["load"] = int(selected_worker.get("load", 0)) + 1

        if not conn:
            with self._tasks_lock:
                task.status = TaskStatus.PENDING.value
                task.assigned_worker = None
                task.assigned_at = 0.0
            return False

        assign_msg = Message(
            msg_type=MessageType.TASK_ASSIGN.value,
            sender_id=self.node_id,
            payload={
                "task_id": task.task_id,
                "description": task.description,
                "client_id": task.client_id,
            },
            lamport_time=self.clock.send_event(),
        )

        try:
            self._send_worker_message(selected_id, conn, assign_msg.to_json())
            self._log_send(assign_msg, f"worker:{selected_id}")
            log_event(
                self.logger,
                self.clock.get_time(),
                "TASK_DISTRIBUTED",
                f"Tarefa '{task.task_id}' -> worker '{selected_id}' {selection_reason}",
            )
            self._sync_state_to_backup()
            return True
        except Exception as e:
            log_event(
                self.logger,
                self.clock.tick(),
                "DISTRIBUTION_ERROR",
                f"Erro ao enviar tarefa '{task.task_id}' para worker '{selected_id}': {e}",
            )
            with self._tasks_lock:
                task.status = TaskStatus.PENDING.value
                task.assigned_worker = None
                task.assigned_at = 0.0
            with self._workers_lock:
                selected_worker = self.workers.get(selected_id)
                if selected_worker:
                    selected_worker["load"] = max(0, int(selected_worker.get("load", 0)) - 1)
            return False

    def _distribute_pending_tasks(self):
        with self._tasks_lock:
            pending = [t for t in self.tasks.values() if t.status == TaskStatus.PENDING.value]

        for task in pending:
            self._distribute_task(task)

    # ==================== HEARTBEAT DE WORKERS ====================

    def _heartbeat_monitor(self):
        heartbeat_timeout = 12
        max_missed_heartbeats = 2
        activity_timeout = 120

        while self.is_running:
            time.sleep(3)
            current_time = time.time()
            to_disconnect = []

            with self._workers_lock:
                for wid, winfo in self.workers.items():
                    if not winfo.get("active"):
                        continue

                    last_hb = float(winfo.get("last_heartbeat", 0))
                    last_register = float(winfo.get("registered_at", 0))
                    last_activity = max(last_hb, last_register)

                    elapsed_since_heartbeat = current_time - last_hb
                    elapsed_since_activity = current_time - last_activity

                    # Se houve atividade recente, reset contagem de heartbeats missed
                    if elapsed_since_activity < activity_timeout:
                        if elapsed_since_heartbeat <= heartbeat_timeout:
                            winfo["missed_heartbeats"] = 0
                            continue
                    else:
                        # Sem atividade recente, considera inativo
                        to_disconnect.append((wid, f"inativo - sem atividade durante {elapsed_since_activity:.1f}s"))
                        continue

                    # Heartbeat expirou, mas há atividade dentro do timeout
                    missed = int(winfo.get("missed_heartbeats", 0)) + 1
                    winfo["missed_heartbeats"] = missed

                    log_event(
                        self.logger,
                        self.clock.tick(),
                        "HEARTBEAT_WARNING",
                        f"Worker '{wid}' sem heartbeat explícito há {elapsed_since_heartbeat:.1f}s (tentativa {missed}/{max_missed_heartbeats}), última atividade há {elapsed_since_activity:.1f}s",
                    )

                    if missed >= max_missed_heartbeats:
                        to_disconnect.append((wid, f"heartbeat timeout ({elapsed_since_heartbeat:.1f}s)"))

            for wid, reason in to_disconnect:
                log_event(
                    self.logger,
                    self.clock.tick(),
                    "WORKER_TIMEOUT",
                    f"Worker '{wid}' será desconectado: {reason}",
                )
                self._handle_worker_disconnect(wid, reason=reason)

    # ==================== HEARTBEAT EXPLÍCITO ENTRE ORQUESTRADORES ====================

    def _send_orchestrator_heartbeats(self):
        """Envia heartbeat periódico ao backup via multicast, separado de STATE_SYNC."""
        while self.is_running:
            time.sleep(5)
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

                heartbeat_msg = Message(
                    msg_type=MessageType.ORCHESTRATOR_HEARTBEAT.value,
                    sender_id=self.node_id,
                    payload={
                        "node_id": self.node_id,
                        "role": self.role,
                        "timestamp": time.time(),
                        "active_workers": len([w for w in self.workers.values() if w.get("active")]),
                        "pending_tasks": len([t for t in self.tasks.values() if t.status == TaskStatus.PENDING.value]),
                    },
                    lamport_time=self.clock.send_event(),
                )

                data = heartbeat_msg.to_bytes()
                sock.sendto(data, (self.multicast_group, self.multicast_port))
                self._log_send(heartbeat_msg, f"multicast_heartbeat")

                sock.close()
            except Exception as e:
                log_event(
                    self.logger,
                    self.clock.tick(),
                    "HEARTBEAT_SEND_ERROR",
                    f"Erro ao enviar heartbeat para backup: {e}",
                )

    # ==================== SINCRONIZAÇÃO ====================

    def _multicast_sync(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

        while self.is_running:
            time.sleep(2)
            try:
                self._send_state_sync(sock)
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "SYNC_ERROR", f"Erro na sincronização multicast: {e}")

        sock.close()

    def _send_state_sync(self, sock: socket.socket):
        state = self._get_global_state()
        sync_msg = Message(
            msg_type=MessageType.STATE_SYNC.value,
            sender_id=self.node_id,
            payload=state,
            lamport_time=self.clock.send_event(),
        )

        data = sync_msg.to_bytes()
        if len(data) < 65000:
            sock.sendto(data, (self.multicast_group, self.multicast_port))
            self._log_send(sync_msg, f"multicast:{self.multicast_group}:{self.multicast_port}")
        else:
            log_event(self.logger, self.clock.tick(), "SYNC_WARNING", "Estado muito grande para multicast")

    def _get_global_state(self) -> dict:
        with self._tasks_lock:
            tasks_state = {tid: t.to_dict() for tid, t in self.tasks.items()}

        with self._workers_lock:
            workers_state = {}
            for wid, w in self.workers.items():
                workers_state[wid] = {
                    "worker_id": wid,
                    "addr": w.get("addr"),
                    "load": w.get("load", 0),
                    "active": w.get("active", False),
                    "last_heartbeat": w.get("last_heartbeat", 0.0),
                    "missed_heartbeats": w.get("missed_heartbeats", 0),
                    "registered_at": w.get("registered_at", 0.0),
                    "primary_endpoint": w.get("primary_endpoint"),
                    "secondary_endpoint": w.get("secondary_endpoint"),
                    "last_lamport": w.get("last_lamport", 0),
                }

        return {
            "tasks": tasks_state,
            "workers": workers_state,
            "rr_index": self.rr_index,
            "auth": self.auth_manager.export_state(),
            "cluster": {
                "active_client_endpoint": {"host": self.host, "port": self.client_port},
                "active_worker_endpoint": {"host": self.host, "port": self.worker_port},
                "primary_node_id": self.node_id,
                "role": self.role,
            },
            "lamport_time": self.clock.get_time(),
            "timestamp": time.time(),
        }

    def _sync_state_to_backup(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            self._send_state_sync(sock)
            sock.close()
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "SYNC_ERROR", f"Erro ao sincronizar com backup: {e}")

    # ==================== REPLICAÇÃO / FAILOVER ====================

    def restore_replicated_state(self, state: dict):
        """Restaura estado global recebido do backup para continuidade pós-failover."""
        with self._tasks_lock:
            restored_tasks = {}
            for tid, tdict in state.get("tasks", {}).items():
                restored_tasks[tid] = Task.from_dict(tdict)
            self.tasks = restored_tasks

        with self._workers_lock:
            self.workers = state.get("workers", {})
            for wid, winfo in self.workers.items():
                winfo["active"] = False
                winfo["load"] = 0
                winfo["missed_heartbeats"] = 0

            self._worker_connections = {}
            self._worker_send_locks = {wid: threading.Lock() for wid in self.workers.keys()}

        self.rr_index = int(state.get("rr_index", 0))
        self.auth_manager.import_state(state.get("auth", {}))

        cluster = state.get("cluster", {})
        restored_lamport = int(state.get("lamport_time", 0))
        self.clock.receive_event(restored_lamport)

        log_event(
            self.logger,
            self.clock.tick(),
            "STATE_RESTORED",
            (
                f"Estado restaurado: tasks={len(self.tasks)} workers={len(self.workers)} "
                f"sessions={len(self.auth_manager.export_state().get('tokens', {}))} "
                f"cluster={cluster}"
            ),
        )

    # ==================== COMUNICAÇÃO TCP ====================

    def _send_worker_message(self, worker_id: str, conn: socket.socket, data: str):
        with self._workers_lock:
            send_lock = self._worker_send_locks.get(worker_id)

        if send_lock is None:
            self._send_message(conn, data)
            return

        with send_lock:
            self._send_message(conn, data)

    @staticmethod
    def _send_message(conn: socket.socket, data: str):
        encoded = data.encode('utf-8')
        length = len(encoded)
        conn.sendall(struct.pack('!I', length) + encoded)

    @staticmethod
    def _recv_message(conn: socket.socket) -> Optional[str]:
        raw_length = Orchestrator._recv_exact(conn, 4)
        if not raw_length:
            return None
        length = struct.unpack('!I', raw_length)[0]

        raw_data = Orchestrator._recv_exact(conn, length)
        if not raw_data:
            return None
        return raw_data.decode('utf-8')

    @staticmethod
    def _recv_exact(conn: socket.socket, n: int) -> Optional[bytes]:
        data = b''
        while len(data) < n:
            chunk = conn.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _log_receive(self, msg: Message, source: str):
        log_event(
            self.logger,
            self.clock.get_time(),
            "MSG_RECV",
            f"type={msg.msg_type} from={source} sender={msg.sender_id} lamport_msg={msg.lamport_time}",
        )

    def _log_send(self, msg: Message, target: str):
        log_event(
            self.logger,
            self.clock.get_time(),
            "MSG_SEND",
            f"type={msg.msg_type} to={target} sender={msg.sender_id} lamport_msg={msg.lamport_time}",
        )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Orquestrador Principal')
    parser.add_argument('--host', default='0.0.0.0', help='Host do servidor')
    parser.add_argument('--client-port', type=int, default=5000, help='Porta para clientes')
    parser.add_argument('--worker-port', type=int, default=5001, help='Porta para workers')
    parser.add_argument('--multicast-group', default='224.1.1.1', help='Grupo multicast')
    parser.add_argument('--multicast-port', type=int, default=5007, help='Porta multicast')

    args = parser.parse_args()

    orchestrator = Orchestrator(
        host=args.host,
        client_port=args.client_port,
        worker_port=args.worker_port,
        multicast_group=args.multicast_group,
        multicast_port=args.multicast_port,
    )
    orchestrator.start()
