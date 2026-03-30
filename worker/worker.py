"""
Worker (Nó de Processamento)
==============================
Responsabilidades:
- Conectar-se ao orquestrador principal
- Executar tarefas enviadas pelo orquestrador
- Reportar status periodicamente (heartbeat)
- Poder falhar de forma simulada (para testes de redistribuição)
"""

import socket
import threading
import json
import time
import struct
import random
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.lamport_clock import LamportClock
from utils.protocol import Message, MessageType, TaskStatus
from utils.logger import setup_logger, log_event


class Worker:
    def __init__(self, worker_id: str, orchestrator_host='127.0.0.1',
                 orchestrator_port=5001, simulate_failure=False,
                 failure_probability=0.2):
        self.worker_id = worker_id
        self.orchestrator_host = orchestrator_host
        self.orchestrator_port = orchestrator_port
        self.simulate_failure = simulate_failure
        self.failure_probability = failure_probability

        # Estado
        self.is_running = True
        self.conn = None
        self.current_tasks = {}  # task_id -> task_info

        # Componentes
        self.clock = LamportClock(worker_id)
        self.logger = setup_logger(worker_id)

        # Lock
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()

        log_event(self.logger, self.clock.tick(), "INIT",
                  f"Worker '{worker_id}' inicializado | "
                  f"Simulação de falha: {simulate_failure} (prob: {failure_probability})")

    def start(self):
        """Inicia o worker."""
        log_event(self.logger, self.clock.tick(), "START",
                  f"Conectando ao orquestrador em {self.orchestrator_host}:{self.orchestrator_port}")

        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect((self.orchestrator_host, self.orchestrator_port))

            log_event(self.logger, self.clock.tick(), "CONNECTED",
                      "Conectado ao orquestrador com sucesso")

            # Registrar no orquestrador
            self._register()

            # Thread para heartbeat
            heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
            heartbeat_thread.start()

            # Loop principal: receber tarefas
            self._receive_loop()

        except ConnectionRefusedError:
            log_event(self.logger, self.clock.tick(), "CONNECTION_FAILED",
                      "Não foi possível conectar ao orquestrador")
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "ERROR",
                      f"Erro no worker: {e}")
        finally:
            self.shutdown()

    def shutdown(self):
        log_event(self.logger, self.clock.tick(), "SHUTDOWN",
                  f"Worker '{self.worker_id}' encerrado")
        self.is_running = False
        if self.conn:
            try:
                self.conn.close()
            except:
                pass

    def _register(self):
        """Registra o worker no orquestrador."""
        msg = Message(
            msg_type=MessageType.WORKER_REGISTER.value,
            sender_id=self.worker_id,
            payload={"worker_id": self.worker_id},
            lamport_time=self.clock.send_event()
        )
        self._send_message(msg.to_json())
        log_event(self.logger, self.clock.get_time(), "REGISTER",
                  "Pedido de registro enviado ao orquestrador")

    def _receive_loop(self):
        """Loop principal para receber mensagens do orquestrador."""
        while self.is_running:
            try:
                data = self._recv_message()
                if not data:
                    log_event(self.logger, self.clock.tick(), "DISCONNECTED",
                              "Conexão com orquestrador perdida")
                    break

                msg = Message.from_json(data)
                self.clock.receive_event(msg.lamport_time)

                if msg.msg_type == MessageType.WORKER_REGISTER_ACK.value:
                    log_event(self.logger, self.clock.get_time(), "REGISTERED",
                              "Registro confirmado pelo orquestrador")

                elif msg.msg_type == MessageType.TASK_ASSIGN.value:
                    self._handle_task_assignment(msg)

                elif msg.msg_type == MessageType.HEARTBEAT_ACK.value:
                    pass  # ACK recebido silenciosamente

                elif msg.msg_type == MessageType.SIMULATE_FAILURE.value:
                    log_event(self.logger, self.clock.tick(), "SIMULATED_FAILURE",
                              "Recebido comando de simulação de falha - encerrando")
                    self.is_running = False
                    break

            except Exception as e:
                if self.is_running:
                    log_event(self.logger, self.clock.tick(), "RECV_ERROR",
                              f"Erro ao receber mensagem: {e}")
                break

    def _handle_task_assignment(self, msg: Message):
        """Processa atribuição de tarefa."""
        task_id = msg.payload.get("task_id")
        description = msg.payload.get("description", "")

        log_event(self.logger, self.clock.tick(), "TASK_RECEIVED",
                  f"Tarefa '{task_id}' recebida | Descrição: {description}")

        # Executar tarefa em thread separada
        task_thread = threading.Thread(
            target=self._execute_task,
            args=(task_id, description),
            daemon=True
        )
        task_thread.start()

    def _execute_task(self, task_id: str, description: str):
        """
        Simula a execução de uma tarefa.
        Pode falhar se a simulação de falha estiver ativada.
        """
        log_event(self.logger, self.clock.tick(), "TASK_EXECUTING",
                  f"Executando tarefa '{task_id}': {description}")

        # Simular tempo de processamento (2 a 8 segundos)
        processing_time = random.uniform(2, 8)

        # Verificar simulação de falha
        if self.simulate_failure and random.random() < self.failure_probability:
            # Simular falha no meio da execução
            time.sleep(processing_time / 2)
            log_event(self.logger, self.clock.tick(), "TASK_FAILURE_SIMULATED",
                      f"FALHA SIMULADA na tarefa '{task_id}'!")

            fail_msg = Message(
                msg_type=MessageType.TASK_FAILED.value,
                sender_id=self.worker_id,
                payload={
                    "task_id": task_id,
                    "reason": "Falha simulada durante o processamento"
                },
                lamport_time=self.clock.send_event()
            )
            try:
                self._send_message(fail_msg.to_json())
            except:
                pass
            return

        # Processamento normal
        time.sleep(processing_time)

        # Gerar resultado simulado
        result = f"Resultado da tarefa '{description}' processada por {self.worker_id} em {processing_time:.1f}s"

        log_event(self.logger, self.clock.tick(), "TASK_DONE",
                  f"Tarefa '{task_id}' concluída em {processing_time:.1f}s")

        # Enviar resultado
        complete_msg = Message(
            msg_type=MessageType.TASK_COMPLETE.value,
            sender_id=self.worker_id,
            payload={
                "task_id": task_id,
                "result": result
            },
            lamport_time=self.clock.send_event()
        )

        try:
            self._send_message(complete_msg.to_json())
            log_event(self.logger, self.clock.get_time(), "TASK_REPORTED",
                      f"Resultado da tarefa '{task_id}' enviado ao orquestrador")
        except Exception as e:
            log_event(self.logger, self.clock.tick(), "REPORT_ERROR",
                      f"Erro ao reportar conclusão da tarefa '{task_id}': {e}")

    def _send_heartbeats(self):
        """Envia heartbeats periódicos ao orquestrador."""
        while self.is_running:
            time.sleep(3)  # Heartbeat a cada 3 segundos

            try:
                hb_msg = Message(
                    msg_type=MessageType.HEARTBEAT.value,
                    sender_id=self.worker_id,
                    payload={"timestamp": time.time()},
                    lamport_time=self.clock.send_event()
                )
                self._send_message(hb_msg.to_json())
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "HEARTBEAT_ERROR",
                          f"Erro ao enviar heartbeat: {e}")
                break

    def _send_message(self, data: str):
        """Envia mensagem TCP com prefixo de tamanho."""
        encoded = data.encode('utf-8')
        length = len(encoded)
        with self._send_lock:
            self.conn.sendall(struct.pack('!I', length) + encoded)

    def _recv_message(self) -> str:
        """Recebe mensagem TCP com prefixo de tamanho."""
        raw_length = self._recv_exact(4)
        if not raw_length:
            return None
        length = struct.unpack('!I', raw_length)[0]

        raw_data = self._recv_exact(length)
        if not raw_data:
            return None
        return raw_data.decode('utf-8')

    def _recv_exact(self, n: int) -> bytes:
        """Recebe exatamente n bytes."""
        data = b''
        while len(data) < n:
            chunk = self.conn.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('--id', required=True, help='ID do worker (ex: worker_1)')
    parser.add_argument('--host', default='127.0.0.1', help='Host do orquestrador')
    parser.add_argument('--port', type=int, default=5001, help='Porta do orquestrador para workers')
    parser.add_argument('--simulate-failure', action='store_true',
                        help='Ativar simulação de falhas aleatórias')
    parser.add_argument('--failure-prob', type=float, default=0.2,
                        help='Probabilidade de falha simulada (0.0-1.0)')

    args = parser.parse_args()

    worker = Worker(
        worker_id=args.id,
        orchestrator_host=args.host,
        orchestrator_port=args.port,
        simulate_failure=args.simulate_failure,
        failure_probability=args.failure_prob
    )
    worker.start()
