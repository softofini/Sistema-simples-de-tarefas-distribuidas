"""
Worker (Nó de Processamento)
==============================
Responsabilidades:
- Conectar-se ao endpoint primário e ao secundário (failover automático)
- Registrar-se novamente após reconexão
- Retomar recebimento de tarefas após failover
- Reportar início real de execução (TASK_STARTED)
- Enviar heartbeat periódico
"""

import socket
import threading
import time
import struct
import random
import sys
import os
from typing import List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.lamport_clock import LamportClock
from utils.protocol import Message, MessageType
from utils.logger import setup_logger, log_event


class Worker:
    def __init__(
        self,
        worker_id: str,
        orchestrator_host: str = '127.0.0.1',
        orchestrator_port: int = 5001,
        secondary_host: str = '127.0.0.1',
        secondary_port: int = 6001,
        simulate_failure: bool = False,
        failure_probability: float = 0.2,
    ):
        self.worker_id = worker_id
        self.primary_endpoint = (orchestrator_host, orchestrator_port)
        self.secondary_endpoint = (secondary_host, secondary_port)
        self._preferred_endpoint = self.primary_endpoint

        self.simulate_failure = simulate_failure
        self.failure_probability = failure_probability

        self.is_running = True
        self.conn: Optional[socket.socket] = None
        self.current_tasks = {}

        self.clock = LamportClock(worker_id)
        self.logger = setup_logger(worker_id)

        self._state_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._connection_generation = 0

        log_event(
            self.logger,
            self.clock.tick(),
            "INIT",
            (
                f"Worker '{worker_id}' inicializado | primary={self.primary_endpoint} "
                f"secondary={self.secondary_endpoint} simulate_failure={simulate_failure}"
            ),
        )

    def start(self):
        """Inicia loop resiliente de conexão e processamento."""
        log_event(self.logger, self.clock.tick(), "START", "Iniciando worker com reconexão automática")

        try:
            while self.is_running:
                if not self._connect_to_orchestrator():
                    time.sleep(2)
                    continue

                with self._state_lock:
                    self._connection_generation += 1
                    generation = self._connection_generation

                if not self._register():
                    self._close_connection()
                    time.sleep(1)
                    continue

                heartbeat_thread = threading.Thread(
                    target=self._send_heartbeats,
                    args=(generation,),
                    daemon=True,
                )
                heartbeat_thread.start()

                disconnect_reason = self._receive_loop(generation)
                if not self.is_running:
                    break

                log_event(
                    self.logger,
                    self.clock.tick(),
                    "RECONNECT_ATTEMPT",
                    f"Conexão perdida ({disconnect_reason}). Tentando reconectar...",
                )
                self._close_connection()
                time.sleep(1.5)

        except Exception as e:
            log_event(self.logger, self.clock.tick(), "ERROR", f"Erro no worker: {e}")
        finally:
            self.shutdown()

    def shutdown(self):
        self.is_running = False
        self._close_connection()
        log_event(self.logger, self.clock.tick(), "SHUTDOWN", f"Worker '{self.worker_id}' encerrado")

    # ==================== CONEXÃO E FAILOVER ====================

    def _ordered_endpoints(self) -> List[Tuple[str, int]]:
        endpoints = [self._preferred_endpoint, self.primary_endpoint, self.secondary_endpoint]
        ordered = []
        for endpoint in endpoints:
            if endpoint not in ordered:
                ordered.append(endpoint)
        return ordered

    def _connect_to_orchestrator(self) -> bool:
        for host, port in self._ordered_endpoints():
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.settimeout(8)
                conn.connect((host, port))
                conn.settimeout(0.35)
                with self._state_lock:
                    self.conn = conn
                    self._preferred_endpoint = (host, port)

                redirected = self._consume_redirect_if_any()
                if redirected:
                    continue

                conn.settimeout(None)

                log_event(
                    self.logger,
                    self.clock.tick(),
                    "CONNECTED",
                    f"Conectado ao orquestrador em {host}:{port}",
                )
                return True
            except (ConnectionRefusedError, OSError, TimeoutError):
                continue

        log_event(
            self.logger,
            self.clock.tick(),
            "CONNECTION_FAILED",
            f"Falha ao conectar em endpoints {self._ordered_endpoints()}",
        )
        return False

    def _close_connection(self):
        with self._state_lock:
            conn = self.conn
            self.conn = None
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # ==================== PROTOCOLO ====================

    def _register(self) -> bool:
        msg = Message(
            msg_type=MessageType.WORKER_REGISTER.value,
            sender_id=self.worker_id,
            payload={
                "worker_id": self.worker_id,
                "primary_endpoint": {"host": self.primary_endpoint[0], "port": self.primary_endpoint[1]},
                "secondary_endpoint": {"host": self.secondary_endpoint[0], "port": self.secondary_endpoint[1]},
            },
            lamport_time=self.clock.send_event(),
        )
        try:
            self._send_message(msg.to_json())
            self._log_send(msg, "orchestrator")
            log_event(self.logger, self.clock.get_time(), "REGISTER", "Pedido de registro enviado")
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            log_event(self.logger, self.clock.tick(), "REGISTER_RETRY", f"Falha ao registrar (reconectando): {e}")
            return False

    def _consume_redirect_if_any(self) -> bool:
        """Consome REDIRECT imediato enviado por backup passivo sem derrubar o processo."""
        try:
            data = self._recv_message()
        except socket.timeout:
            return False
        except Exception:
            return False

        if not data:
            return False

        try:
            msg = Message.from_json(data)
        except Exception:
            return False

        if msg.msg_type != MessageType.REDIRECT.value:
            return False

        self.clock.receive_event(msg.lamport_time)
        host = msg.payload.get("host")
        port = msg.payload.get("port")
        if host and port:
            self._preferred_endpoint = (host, int(port))
            log_event(
                self.logger,
                self.clock.tick(),
                "REDIRECT_RECEIVED",
                f"Redirecionamento recebido para {host}:{port}",
            )

        self._close_connection()
        return True

    def _receive_loop(self, generation: int) -> str:
        while self.is_running:
            with self._state_lock:
                if generation != self._connection_generation:
                    return "geração de conexão inválida"

            try:
                data = self._recv_message()
                if not data:
                    return "socket fechado"

                msg = Message.from_json(data)
                self.clock.receive_event(msg.lamport_time)
                self._log_recv(msg, "orchestrator")

                if msg.msg_type == MessageType.WORKER_REGISTER_ACK.value:
                    log_event(self.logger, self.clock.get_time(), "REGISTERED", "Registro confirmado")

                elif msg.msg_type == MessageType.TASK_ASSIGN.value:
                    self._handle_task_assignment(msg)

                elif msg.msg_type == MessageType.HEARTBEAT_ACK.value:
                    pass

                elif msg.msg_type == MessageType.REDIRECT.value:
                    host = msg.payload.get("host")
                    port = msg.payload.get("port")
                    if host and port:
                        self._preferred_endpoint = (host, int(port))
                        log_event(
                            self.logger,
                            self.clock.tick(),
                            "REDIRECT_RECEIVED",
                            f"Redirecionado para endpoint {host}:{port}",
                        )
                        return "redirect recebido"

                elif msg.msg_type == MessageType.SIMULATE_FAILURE.value:
                    log_event(self.logger, self.clock.tick(), "SIMULATED_FAILURE", "Comando de falha recebido")
                    self.is_running = False
                    return "falha simulada"

            except Exception as e:
                if self.is_running:
                    log_event(self.logger, self.clock.tick(), "RECV_ERROR", f"Erro ao receber mensagem: {e}")
                return str(e)

        return "worker encerrado"

    def _handle_task_assignment(self, msg: Message):
        task_id = msg.payload.get("task_id")
        description = msg.payload.get("description", "")

        if not task_id:
            return

        with self._state_lock:
            self.current_tasks[task_id] = {
                "description": description,
                "status": "ASSIGNED",
                "received_at": time.time(),
            }

        log_event(self.logger, self.clock.tick(), "TASK_RECEIVED", f"Tarefa '{task_id}' recebida")

        task_thread = threading.Thread(target=self._execute_task, args=(task_id, description), daemon=True)
        task_thread.start()

    def _execute_task(self, task_id: str, description: str):
        started_msg = Message(
            msg_type=MessageType.TASK_STARTED.value,
            sender_id=self.worker_id,
            payload={"task_id": task_id},
            lamport_time=self.clock.send_event(),
        )

        try:
            self._send_message(started_msg.to_json())
            self._log_send(started_msg, "orchestrator")
            with self._state_lock:
                if task_id in self.current_tasks:
                    self.current_tasks[task_id]["status"] = "RUNNING"
            log_event(self.logger, self.clock.get_time(), "TASK_RUNNING", f"Tarefa '{task_id}' iniciou execução real")
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "TASK_START_REPORT_ERROR", f"Falha ao sinalizar RUNNING: {e}")
            return

        processing_time = random.uniform(2, 8)

        if self.simulate_failure and random.random() < self.failure_probability:
            time.sleep(processing_time / 2)
            log_event(self.logger, self.clock.tick(), "TASK_FAILURE_SIMULATED", f"Falha simulada em '{task_id}'")

            fail_msg = Message(
                msg_type=MessageType.TASK_FAILED.value,
                sender_id=self.worker_id,
                payload={"task_id": task_id, "reason": "Falha simulada durante o processamento"},
                lamport_time=self.clock.send_event(),
            )
            try:
                self._send_message(fail_msg.to_json())
                self._log_send(fail_msg, "orchestrator")
            except Exception:
                pass
            with self._state_lock:
                self.current_tasks.pop(task_id, None)
            return

        time.sleep(processing_time)

        result = (
            f"Resultado da tarefa '{description}' processada por {self.worker_id} "
            f"em {processing_time:.1f}s"
        )

        complete_msg = Message(
            msg_type=MessageType.TASK_COMPLETE.value,
            sender_id=self.worker_id,
            payload={"task_id": task_id, "result": result},
            lamport_time=self.clock.send_event(),
        )

        try:
            self._send_message(complete_msg.to_json())
            self._log_send(complete_msg, "orchestrator")
            log_event(self.logger, self.clock.get_time(), "TASK_DONE", f"Tarefa '{task_id}' concluída")
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "REPORT_ERROR", f"Erro ao reportar '{task_id}': {e}")
        finally:
            with self._state_lock:
                self.current_tasks.pop(task_id, None)

    def _send_heartbeats(self, generation: int):
        while self.is_running:
            time.sleep(3)

            with self._state_lock:
                if generation != self._connection_generation or self.conn is None:
                    return

            hb_msg = Message(
                msg_type=MessageType.HEARTBEAT.value,
                sender_id=self.worker_id,
                payload={"timestamp": time.time(), "active_tasks": len(self.current_tasks)},
                lamport_time=self.clock.send_event(),
            )

            try:
                self._send_message(hb_msg.to_json())
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "HEARTBEAT_ERROR", f"Erro ao enviar heartbeat: {e}")
                return

    # ==================== TCP ====================

    def _send_message(self, data: str):
        with self._state_lock:
            conn = self.conn
        if conn is None:
            raise ConnectionError("Sem conexão com orquestrador")

        encoded = data.encode('utf-8')
        packet = struct.pack('!I', len(encoded)) + encoded
        with self._send_lock:
            conn.sendall(packet)

    def _recv_message(self) -> Optional[str]:
        with self._state_lock:
            conn = self.conn
        if conn is None:
            return None

        raw_length = self._recv_exact(conn, 4)
        if not raw_length:
            return None
        length = struct.unpack('!I', raw_length)[0]

        raw_data = self._recv_exact(conn, length)
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

    def _log_send(self, msg: Message, target: str):
        log_event(
            self.logger,
            self.clock.get_time(),
            "MSG_SEND",
            f"type={msg.msg_type} to={target} sender={msg.sender_id} lamport_msg={msg.lamport_time}",
        )

    def _log_recv(self, msg: Message, source: str):
        log_event(
            self.logger,
            self.clock.get_time(),
            "MSG_RECV",
            f"type={msg.msg_type} from={source} sender={msg.sender_id} lamport_msg={msg.lamport_time}",
        )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('--id', required=True, help='ID do worker (ex: worker_1)')
    parser.add_argument('--host', default='127.0.0.1', help='Host primário do orquestrador')
    parser.add_argument('--port', type=int, default=5001, help='Porta primária para workers')
    parser.add_argument('--secondary-host', default='127.0.0.1', help='Host secundário (backup)')
    parser.add_argument('--secondary-port', type=int, default=6001, help='Porta secundária para workers')
    parser.add_argument('--simulate-failure', action='store_true', help='Ativar simulação de falhas aleatórias')
    parser.add_argument('--failure-prob', type=float, default=0.2, help='Probabilidade de falha simulada (0.0-1.0)')

    args = parser.parse_args()

    worker = Worker(
        worker_id=args.id,
        orchestrator_host=args.host,
        orchestrator_port=args.port,
        secondary_host=args.secondary_host,
        secondary_port=args.secondary_port,
        simulate_failure=args.simulate_failure,
        failure_probability=args.failure_prob,
    )
    worker.start()
