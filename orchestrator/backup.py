"""
Orquestrador Secundário (Backup)
==================================
Responsabilidades:
- Manter cópia sincronizada do estado global via UDP Multicast
- Assumir o papel principal caso o coordenador falhe (failover)
- Monitorar saúde do orquestrador principal via heartbeat
"""

import socket
import threading
import json
import time
import struct
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.lamport_clock import LamportClock
from utils.protocol import Message, MessageType, Task, TaskStatus, AuthManager
from utils.logger import setup_logger, log_event


class OrchestratorBackup:
    def __init__(self, host='0.0.0.0', client_port=6000, worker_port=6001,
                 multicast_group='224.1.1.1', multicast_port=5007,
                 primary_host='127.0.0.1', primary_client_port=5000):
        # Configurações de rede
        self.host = host
        self.client_port = client_port
        self.worker_port = worker_port
        self.multicast_group = multicast_group
        self.multicast_port = multicast_port
        self.primary_host = primary_host
        self.primary_client_port = primary_client_port

        # Estado replicado do primário
        self.tasks = {}  # task_id -> Task dict
        self.workers = {}  # worker_id -> worker info
        self.rr_index = 0

        # Estado local
        self.is_primary = False
        self.is_running = True
        self.primary_alive = True
        self.last_primary_heartbeat = time.time()

        # Componentes
        self.clock = LamportClock("orchestrator_backup")
        self.auth_manager = AuthManager()
        self.logger = setup_logger("orchestrator_backup")

        # Locks
        self._state_lock = threading.Lock()

        log_event(self.logger, self.clock.tick(), "INIT",
                  "Orquestrador backup inicializado")

    def start(self):
        """Inicia serviços do backup."""
        log_event(self.logger, self.clock.tick(), "START",
                  "Iniciando serviços do orquestrador backup")

        # Thread para receber estado via multicast
        multicast_thread = threading.Thread(target=self._receive_multicast, daemon=True)
        multicast_thread.start()

        # Thread para monitorar o primário
        monitor_thread = threading.Thread(target=self._monitor_primary, daemon=True)
        monitor_thread.start()

        log_event(self.logger, self.clock.tick(), "RUNNING",
                  "Orquestrador backup ativo - aguardando sincronização")

        try:
            while self.is_running:
                time.sleep(1)
                if self.is_primary:
                    log_event(self.logger, self.clock.tick(), "PRIMARY_MODE",
                              "Backup agora operando como PRIMÁRIO")
                    self._run_as_primary()
                    break
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        log_event(self.logger, self.clock.tick(), "SHUTDOWN",
                  "Encerrando orquestrador backup")
        self.is_running = False

    def _receive_multicast(self):
        """Recebe estado global do primário via UDP Multicast."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self.multicast_port))
        sock.settimeout(2)

        # Juntar-se ao grupo multicast
        mreq = struct.pack("4sl", socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        log_event(self.logger, self.clock.tick(), "MULTICAST_READY",
                  f"Ouvindo multicast em {self.multicast_group}:{self.multicast_port}")

        while self.is_running and not self.is_primary:
            try:
                data, addr = sock.recvfrom(65535)
                msg = Message.from_bytes(data)
                self.clock.receive_event(msg.lamport_time)

                if msg.msg_type == MessageType.STATE_SYNC.value:
                    self._update_state(msg.payload)
                    self.last_primary_heartbeat = time.time()
                    self.primary_alive = True

                    log_event(self.logger, self.clock.get_time(), "STATE_SYNCED",
                              f"Estado sincronizado do primário | "
                              f"Tarefas: {len(self.tasks)} | Workers: {len(self.workers)}")

            except socket.timeout:
                continue
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "MULTICAST_ERROR",
                          f"Erro ao receber multicast: {e}")

        sock.close()

    def _update_state(self, state: dict):
        """Atualiza o estado local com dados do primário."""
        with self._state_lock:
            self.tasks = state.get("tasks", {})
            self.workers = state.get("workers", {})
            self.rr_index = state.get("rr_index", 0)

    def _monitor_primary(self):
        """Monitora se o primário está ativo."""
        FAILOVER_TIMEOUT = 15  # segundos sem sync = primário caiu

        while self.is_running and not self.is_primary:
            time.sleep(3)
            elapsed = time.time() - self.last_primary_heartbeat

            if elapsed > FAILOVER_TIMEOUT and self.primary_alive:
                log_event(self.logger, self.clock.tick(), "PRIMARY_FAILURE",
                          f"Orquestrador primário sem resposta por {elapsed:.0f}s")
                log_event(self.logger, self.clock.tick(), "FAILOVER_ACTIVATE",
                          "ATIVANDO FAILOVER - Backup assumindo como PRIMÁRIO")
                self.primary_alive = False
                self.is_primary = True
                break

            elif elapsed > FAILOVER_TIMEOUT / 2:
                log_event(self.logger, self.clock.tick(), "PRIMARY_WARNING",
                          f"Primário sem heartbeat há {elapsed:.0f}s (timeout em {FAILOVER_TIMEOUT}s)")

    def _run_as_primary(self):
        """Executa como orquestrador principal após failover."""
        from orchestrator.orchestrator import Orchestrator

        log_event(self.logger, self.clock.tick(), "FAILOVER_START",
                  f"Iniciando operação como primário em {self.host}:{self.client_port}")

        # Criar um novo orquestrador com o estado replicado
        new_orchestrator = Orchestrator(
            host=self.host,
            client_port=self.client_port,
            worker_port=self.worker_port,
            multicast_group=self.multicast_group,
            multicast_port=self.multicast_port
        )

        # Restaurar estado
        with self._state_lock:
            for tid, tdict in self.tasks.items():
                new_orchestrator.tasks[tid] = Task.from_dict(tdict)
            new_orchestrator.rr_index = self.rr_index

        log_event(self.logger, self.clock.tick(), "FAILOVER_COMPLETE",
                  f"Failover concluído. Operando como primário com "
                  f"{len(self.tasks)} tarefas restauradas")

        new_orchestrator.start()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Orquestrador Backup')
    parser.add_argument('--host', default='0.0.0.0', help='Host do servidor')
    parser.add_argument('--client-port', type=int, default=6000, help='Porta para clientes (failover)')
    parser.add_argument('--worker-port', type=int, default=6001, help='Porta para workers (failover)')
    parser.add_argument('--multicast-group', default='224.1.1.1', help='Grupo multicast')
    parser.add_argument('--multicast-port', type=int, default=5007, help='Porta multicast')

    args = parser.parse_args()

    backup = OrchestratorBackup(
        host=args.host,
        client_port=args.client_port,
        worker_port=args.worker_port,
        multicast_group=args.multicast_group,
        multicast_port=args.multicast_port
    )
    backup.start()
