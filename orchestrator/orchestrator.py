"""
Orquestrador Principal (Coordenador)
=====================================
Responsabilidades:
- Receber tarefas de clientes autenticados via TCP
- Distribuir tarefas para workers usando política de balanceamento (Round Robin)
- Manter checkpoint de tarefas em execução
- Reatribuir tarefas em caso de falha de worker
- Monitorar workers via heartbeat
- Sincronizar estado com o orquestrador backup via UDP Multicast
"""

import socket
import threading
import json
import time
import uuid
import struct
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.lamport_clock import LamportClock
from utils.protocol import Message, MessageType, Task, TaskStatus, AuthManager
from utils.logger import setup_logger, log_event


class Orchestrator:
    def __init__(self, host='0.0.0.0', client_port=5000, worker_port=5001,
                 multicast_group='224.1.1.1', multicast_port=5007):
        # Configurações de rede
        self.host = host
        self.client_port = client_port
        self.worker_port = worker_port
        self.multicast_group = multicast_group
        self.multicast_port = multicast_port

        # Estado do sistema
        self.tasks = {}  # task_id -> Task
        self.workers = {}  # worker_id -> {addr, load, last_heartbeat, active}
        self.is_primary = True
        self.is_running = True

        # Balanceamento Round Robin
        self.rr_index = 0

        # Componentes
        self.clock = LamportClock("orchestrator_primary")
        self.auth_manager = AuthManager()
        self.logger = setup_logger("orchestrator_primary")

        # Locks
        self._tasks_lock = threading.Lock()
        self._workers_lock = threading.Lock()

        log_event(self.logger, self.clock.tick(), "INIT",
                  f"Orquestrador principal inicializado em {host}:{client_port}")

    def start(self):
        """Inicia todos os serviços do orquestrador."""
        log_event(self.logger, self.clock.tick(), "START",
                  "Iniciando serviços do orquestrador principal")

        # Thread para aceitar clientes TCP
        client_thread = threading.Thread(target=self._accept_clients, daemon=True)
        client_thread.start()

        # Thread para aceitar workers TCP
        worker_thread = threading.Thread(target=self._accept_workers, daemon=True)
        worker_thread.start()

        # Thread para heartbeat dos workers
        heartbeat_thread = threading.Thread(target=self._heartbeat_monitor, daemon=True)
        heartbeat_thread.start()

        # Thread para sincronização multicast com backup
        sync_thread = threading.Thread(target=self._multicast_sync, daemon=True)
        sync_thread.start()

        log_event(self.logger, self.clock.tick(), "RUNNING",
                  "Todos os serviços do orquestrador estão ativos")

        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        """Encerra o orquestrador de forma limpa."""
        log_event(self.logger, self.clock.tick(), "SHUTDOWN",
                  "Encerrando orquestrador principal")
        self.is_running = False

    # ==================== GERENCIAMENTO DE CLIENTES ====================

    def _accept_clients(self):
        """Aceita conexões TCP de clientes."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.client_port))
        server.listen(10)
        server.settimeout(2)

        log_event(self.logger, self.clock.tick(), "CLIENT_SERVER",
                  f"Servidor de clientes ouvindo em {self.host}:{self.client_port}")

        while self.is_running:
            try:
                conn, addr = server.accept()
                log_event(self.logger, self.clock.tick(), "CLIENT_CONNECT",
                          f"Nova conexão de cliente: {addr}")
                handler = threading.Thread(
                    target=self._handle_client, args=(conn, addr), daemon=True
                )
                handler.start()
            except socket.timeout:
                continue
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "ERROR",
                          f"Erro ao aceitar cliente: {e}")

        server.close()

    def _handle_client(self, conn: socket.socket, addr):
        """Processa mensagens de um cliente."""
        authenticated_token = None
        try:
            while self.is_running:
                data = self._recv_message(conn)
                if not data:
                    break

                msg = Message.from_json(data)
                self.clock.receive_event(msg.lamport_time)

                if msg.msg_type == MessageType.AUTH_REQUEST.value:
                    self._handle_auth(conn, msg, addr)
                    if msg.payload.get("username"):
                        authenticated_token = msg.token

                elif msg.msg_type == MessageType.TASK_SUBMIT.value:
                    self._handle_task_submit(conn, msg, addr)

                elif msg.msg_type == MessageType.TASK_STATUS_REQUEST.value:
                    self._handle_task_status(conn, msg, addr)

                else:
                    log_event(self.logger, self.clock.tick(), "UNKNOWN_MSG",
                              f"Mensagem desconhecida de {addr}: {msg.msg_type}")

        except Exception as e:
            log_event(self.logger, self.clock.tick(), "CLIENT_ERROR",
                      f"Erro com cliente {addr}: {e}")
        finally:
            conn.close()
            log_event(self.logger, self.clock.tick(), "CLIENT_DISCONNECT",
                      f"Cliente desconectado: {addr}")

    def _handle_auth(self, conn, msg, addr):
        """Processa requisição de autenticação."""
        username = msg.payload.get("username", "")
        password = msg.payload.get("password", "")

        token = self.auth_manager.authenticate(username, password)

        if token:
            response = Message(
                msg_type=MessageType.AUTH_RESPONSE.value,
                sender_id="orchestrator_primary",
                payload={"success": True, "token": token, "message": "Autenticação bem-sucedida"},
                lamport_time=self.clock.send_event()
            )
            log_event(self.logger, self.clock.get_time(), "AUTH_SUCCESS",
                      f"Usuário '{username}' autenticado de {addr}")
        else:
            response = Message(
                msg_type=MessageType.AUTH_RESPONSE.value,
                sender_id="orchestrator_primary",
                payload={"success": False, "token": None, "message": "Credenciais inválidas"},
                lamport_time=self.clock.send_event()
            )
            log_event(self.logger, self.clock.get_time(), "AUTH_FAILED",
                      f"Falha na autenticação do usuário '{username}' de {addr}")

        self._send_message(conn, response.to_json())

    def _handle_task_submit(self, conn, msg, addr):
        """Processa submissão de tarefa."""
        token = msg.token

        # Validar token
        username = self.auth_manager.validate_token(token) if token else None
        if not username:
            response = Message(
                msg_type=MessageType.TASK_SUBMIT_ACK.value,
                sender_id="orchestrator_primary",
                payload={"success": False, "message": "Token inválido. Autentique-se primeiro."},
                lamport_time=self.clock.send_event()
            )
            self._send_message(conn, response.to_json())
            log_event(self.logger, self.clock.get_time(), "TASK_REJECTED",
                      f"Tarefa rejeitada de {addr}: token inválido")
            return

        # Criar tarefa
        task_id = str(uuid.uuid4())[:8]
        task = Task(
            task_id=task_id,
            client_id=username,
            description=msg.payload.get("description", "Tarefa sem descrição"),
            status=TaskStatus.PENDING.value,
            created_at=time.time(),
            lamport_created=self.clock.get_time()
        )

        with self._tasks_lock:
            self.tasks[task_id] = task

        log_event(self.logger, self.clock.tick(), "TASK_SUBMITTED",
                  f"Tarefa '{task_id}' submetida por '{username}' | Descrição: {task.description}")

        # Tentar distribuir imediatamente
        assigned = self._distribute_task(task)

        response = Message(
            msg_type=MessageType.TASK_SUBMIT_ACK.value,
            sender_id="orchestrator_primary",
            payload={
                "success": True,
                "task_id": task_id,
                "status": task.status,
                "message": f"Tarefa '{task_id}' recebida e {'distribuída' if assigned else 'aguardando worker'}"
            },
            lamport_time=self.clock.send_event()
        )
        self._send_message(conn, response.to_json())

        # Sincronizar estado com backup
        self._sync_state_to_backup()

    def _handle_task_status(self, conn, msg, addr):
        """Retorna status de tarefas do cliente."""
        token = msg.token
        username = self.auth_manager.validate_token(token) if token else None

        if not username:
            response = Message(
                msg_type=MessageType.TASK_STATUS_RESPONSE.value,
                sender_id="orchestrator_primary",
                payload={"success": False, "message": "Token inválido"},
                lamport_time=self.clock.send_event()
            )
            self._send_message(conn, response.to_json())
            return

        # Filtrar tarefas do usuário
        task_id = msg.payload.get("task_id")
        with self._tasks_lock:
            if task_id:
                task = self.tasks.get(task_id)
                if task and task.client_id == username:
                    tasks_info = [task.to_dict()]
                else:
                    tasks_info = []
            else:
                tasks_info = [
                    t.to_dict() for t in self.tasks.values()
                    if t.client_id == username
                ]

        response = Message(
            msg_type=MessageType.TASK_STATUS_RESPONSE.value,
            sender_id="orchestrator_primary",
            payload={"success": True, "tasks": tasks_info},
            lamport_time=self.clock.send_event()
        )
        self._send_message(conn, response.to_json())

        log_event(self.logger, self.clock.get_time(), "TASK_STATUS_QUERY",
                  f"Consulta de status por '{username}' de {addr}")

    # ==================== GERENCIAMENTO DE WORKERS ====================

    def _accept_workers(self):
        """Aceita conexões TCP de workers."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.worker_port))
        server.listen(10)
        server.settimeout(2)

        log_event(self.logger, self.clock.tick(), "WORKER_SERVER",
                  f"Servidor de workers ouvindo em {self.host}:{self.worker_port}")

        while self.is_running:
            try:
                conn, addr = server.accept()
                log_event(self.logger, self.clock.tick(), "WORKER_CONNECT",
                          f"Nova conexão de worker: {addr}")
                handler = threading.Thread(
                    target=self._handle_worker, args=(conn, addr), daemon=True
                )
                handler.start()
            except socket.timeout:
                continue
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "ERROR",
                          f"Erro ao aceitar worker: {e}")

        server.close()

    def _handle_worker(self, conn: socket.socket, addr):
        """Processa mensagens de um worker."""
        worker_id = None
        try:
            while self.is_running:
                data = self._recv_message(conn)
                if not data:
                    break

                msg = Message.from_json(data)
                self.clock.receive_event(msg.lamport_time)

                if msg.msg_type == MessageType.WORKER_REGISTER.value:
                    worker_id = msg.sender_id
                    with self._workers_lock:
                        self.workers[worker_id] = {
                            'conn': conn,
                            'addr': addr,
                            'load': 0,
                            'last_heartbeat': time.time(),
                            'active': True
                        }
                    log_event(self.logger, self.clock.tick(), "WORKER_REGISTERED",
                              f"Worker '{worker_id}' registrado de {addr}")

                    # Responder registro
                    response = Message(
                        msg_type=MessageType.WORKER_REGISTER_ACK.value,
                        sender_id="orchestrator_primary",
                        payload={"success": True, "message": "Worker registrado com sucesso"},
                        lamport_time=self.clock.send_event()
                    )
                    self._send_message(conn, response.to_json())

                    # Distribuir tarefas pendentes
                    self._distribute_pending_tasks()

                elif msg.msg_type == MessageType.HEARTBEAT.value:
                    if worker_id and worker_id in self.workers:
                        with self._workers_lock:
                            self.workers[worker_id]['last_heartbeat'] = time.time()
                            self.workers[worker_id]['active'] = True
                        # ACK do heartbeat
                        response = Message(
                            msg_type=MessageType.HEARTBEAT_ACK.value,
                            sender_id="orchestrator_primary",
                            payload={},
                            lamport_time=self.clock.send_event()
                        )
                        self._send_message(conn, response.to_json())

                elif msg.msg_type == MessageType.TASK_COMPLETE.value:
                    task_id = msg.payload.get("task_id")
                    result = msg.payload.get("result", "")
                    self._handle_task_complete(worker_id, task_id, result)

                elif msg.msg_type == MessageType.TASK_FAILED.value:
                    task_id = msg.payload.get("task_id")
                    reason = msg.payload.get("reason", "Falha desconhecida")
                    self._handle_task_failure(worker_id, task_id, reason)

        except Exception as e:
            log_event(self.logger, self.clock.tick(), "WORKER_ERROR",
                      f"Erro com worker '{worker_id}' em {addr}: {e}")
        finally:
            if worker_id:
                self._handle_worker_disconnect(worker_id)
            conn.close()

    def _handle_task_complete(self, worker_id, task_id, result):
        """Processa conclusão de tarefa."""
        with self._tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = TaskStatus.COMPLETED.value
                self.tasks[task_id].result = result
                self.tasks[task_id].completed_at = time.time()
                self.tasks[task_id].lamport_completed = self.clock.get_time()

        with self._workers_lock:
            if worker_id in self.workers:
                self.workers[worker_id]['load'] = max(0, self.workers[worker_id]['load'] - 1)

        log_event(self.logger, self.clock.tick(), "TASK_COMPLETED",
                  f"Tarefa '{task_id}' concluída por worker '{worker_id}' | Resultado: {result}")

        self._sync_state_to_backup()

    def _handle_task_failure(self, worker_id, task_id, reason):
        """Processa falha em tarefa e reatribui."""
        with self._tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = TaskStatus.FAILED.value
                self.tasks[task_id].retries += 1

        with self._workers_lock:
            if worker_id in self.workers:
                self.workers[worker_id]['load'] = max(0, self.workers[worker_id]['load'] - 1)

        log_event(self.logger, self.clock.tick(), "TASK_FAILED",
                  f"Tarefa '{task_id}' falhou no worker '{worker_id}' | Motivo: {reason}")

        # Reatribuir a tarefa
        with self._tasks_lock:
            task = self.tasks.get(task_id)
            if task and task.retries < 3:
                task.status = TaskStatus.REASSIGNED.value
                task.assigned_worker = None
                log_event(self.logger, self.clock.tick(), "TASK_REASSIGN",
                          f"Reatribuindo tarefa '{task_id}' (tentativa {task.retries})")
                self._distribute_task(task)

        self._sync_state_to_backup()

    def _handle_worker_disconnect(self, worker_id):
        """Trata desconexão de um worker."""
        log_event(self.logger, self.clock.tick(), "WORKER_DISCONNECT",
                  f"Worker '{worker_id}' desconectado")

        with self._workers_lock:
            if worker_id in self.workers:
                self.workers[worker_id]['active'] = False

        # Reatribuir tarefas do worker desconectado
        tasks_to_reassign = []
        with self._tasks_lock:
            for task_id, task in self.tasks.items():
                if (task.assigned_worker == worker_id and
                        task.status in [TaskStatus.ASSIGNED.value, TaskStatus.RUNNING.value]):
                    task.status = TaskStatus.REASSIGNED.value
                    task.assigned_worker = None
                    task.retries += 1
                    tasks_to_reassign.append(task)

        for task in tasks_to_reassign:
            log_event(self.logger, self.clock.tick(), "TASK_REASSIGN",
                      f"Reatribuindo tarefa '{task.task_id}' (worker '{worker_id}' caiu)")
            self._distribute_task(task)

        self._sync_state_to_backup()

    # ==================== BALANCEAMENTO DE CARGA ====================

    def _distribute_task(self, task: Task) -> bool:
        """
        Distribui uma tarefa usando política Round Robin.
        Retorna True se a tarefa foi atribuída com sucesso.
        """
        with self._workers_lock:
            active_workers = [
                (wid, winfo) for wid, winfo in self.workers.items()
                if winfo['active']
            ]

        if not active_workers:
            log_event(self.logger, self.clock.tick(), "NO_WORKERS",
                      f"Nenhum worker ativo para tarefa '{task.task_id}'")
            return False

        # Round Robin: seleciona o próximo worker na sequência
        self.rr_index = self.rr_index % len(active_workers)
        selected_id, selected_info = active_workers[self.rr_index]
        self.rr_index = (self.rr_index + 1) % len(active_workers)

        # Atribuir tarefa
        with self._tasks_lock:
            task.status = TaskStatus.ASSIGNED.value
            task.assigned_worker = selected_id

        with self._workers_lock:
            self.workers[selected_id]['load'] += 1

        # Enviar tarefa ao worker
        assign_msg = Message(
            msg_type=MessageType.TASK_ASSIGN.value,
            sender_id="orchestrator_primary",
            payload={
                "task_id": task.task_id,
                "description": task.description,
                "client_id": task.client_id
            },
            lamport_time=self.clock.send_event()
        )

        try:
            conn = selected_info['conn']
            self._send_message(conn, assign_msg.to_json())
            log_event(self.logger, self.clock.get_time(), "TASK_DISTRIBUTED",
                      f"Tarefa '{task.task_id}' distribuída para worker '{selected_id}' (Round Robin)")
            return True
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "DISTRIBUTION_ERROR",
                      f"Erro ao enviar tarefa '{task.task_id}' para worker '{selected_id}': {e}")
            with self._tasks_lock:
                task.status = TaskStatus.PENDING.value
                task.assigned_worker = None
            return False

    def _distribute_pending_tasks(self):
        """Distribui todas as tarefas pendentes."""
        with self._tasks_lock:
            pending = [
                t for t in self.tasks.values()
                if t.status in [TaskStatus.PENDING.value, TaskStatus.REASSIGNED.value]
            ]

        for task in pending:
            self._distribute_task(task)

    # ==================== HEARTBEAT ====================

    def _heartbeat_monitor(self):
        """Monitora heartbeats dos workers."""
        TIMEOUT = 10  # segundos sem heartbeat = worker inativo

        while self.is_running:
            time.sleep(5)
            current_time = time.time()

            with self._workers_lock:
                for wid, winfo in list(self.workers.items()):
                    if winfo['active'] and (current_time - winfo['last_heartbeat'] > TIMEOUT):
                        log_event(self.logger, self.clock.tick(), "HEARTBEAT_TIMEOUT",
                                  f"Worker '{wid}' sem heartbeat por {TIMEOUT}s - marcando como inativo")
                        winfo['active'] = False

                        # Reatribuir tarefas
                        threading.Thread(
                            target=self._handle_worker_disconnect,
                            args=(wid,), daemon=True
                        ).start()

    # ==================== SINCRONIZAÇÃO MULTICAST ====================

    def _multicast_sync(self):
        """Envia estado global via UDP Multicast para o backup."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

        while self.is_running:
            time.sleep(3)  # Sincronizar a cada 3 segundos

            try:
                state = self._get_global_state()
                sync_msg = Message(
                    msg_type=MessageType.STATE_SYNC.value,
                    sender_id="orchestrator_primary",
                    payload=state,
                    lamport_time=self.clock.send_event()
                )
                data = sync_msg.to_bytes()
                # Fragmentar se necessário (UDP tem limite de ~65KB)
                if len(data) < 65000:
                    sock.sendto(data, (self.multicast_group, self.multicast_port))
                else:
                    log_event(self.logger, self.clock.tick(), "SYNC_WARNING",
                              "Estado muito grande para multicast, truncando...")

            except Exception as e:
                log_event(self.logger, self.clock.tick(), "SYNC_ERROR",
                          f"Erro na sincronização multicast: {e}")

    def _get_global_state(self) -> dict:
        """Retorna o estado global do sistema."""
        with self._tasks_lock:
            tasks_state = {tid: t.to_dict() for tid, t in self.tasks.items()}

        with self._workers_lock:
            workers_state = {
                wid: {
                    'load': w['load'],
                    'active': w['active'],
                    'last_heartbeat': w['last_heartbeat']
                }
                for wid, w in self.workers.items()
            }

        return {
            "tasks": tasks_state,
            "workers": workers_state,
            "rr_index": self.rr_index,
            "lamport_time": self.clock.get_time(),
            "timestamp": time.time()
        }

    def _sync_state_to_backup(self):
        """Força sincronização imediata com backup."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

            state = self._get_global_state()
            sync_msg = Message(
                msg_type=MessageType.STATE_SYNC.value,
                sender_id="orchestrator_primary",
                payload=state,
                lamport_time=self.clock.send_event()
            )
            data = sync_msg.to_bytes()
            if len(data) < 65000:
                sock.sendto(data, (self.multicast_group, self.multicast_port))
            sock.close()
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "SYNC_ERROR",
                      f"Erro ao sincronizar com backup: {e}")

    # ==================== COMUNICAÇÃO TCP ====================

    @staticmethod
    def _send_message(conn: socket.socket, data: str):
        """Envia mensagem TCP com prefixo de tamanho."""
        encoded = data.encode('utf-8')
        length = len(encoded)
        conn.sendall(struct.pack('!I', length) + encoded)

    @staticmethod
    def _recv_message(conn: socket.socket) -> str:
        """Recebe mensagem TCP com prefixo de tamanho."""
        # Receber 4 bytes de tamanho
        raw_length = Orchestrator._recv_exact(conn, 4)
        if not raw_length:
            return None
        length = struct.unpack('!I', raw_length)[0]

        # Receber a mensagem completa
        raw_data = Orchestrator._recv_exact(conn, length)
        if not raw_data:
            return None
        return raw_data.decode('utf-8')

    @staticmethod
    def _recv_exact(conn: socket.socket, n: int) -> bytes:
        """Recebe exatamente n bytes."""
        data = b''
        while len(data) < n:
            chunk = conn.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data


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
        multicast_port=args.multicast_port
    )
    orchestrator.start()
