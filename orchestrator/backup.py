"""
Orquestrador Secundário (Backup)
==================================
Responsabilidades:
- Manter cópia sincronizada do estado global via UDP Multicast
- Assumir o papel principal caso o coordenador falhe (failover)
- Preferir as mesmas portas do primário durante failover
- Fornecer redirecionamento automático para clientes/workers quando em modo passivo
"""

import socket
import threading
import time
import struct
import sys
import os
from typing import Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.lamport_clock import LamportClock
from utils.protocol import Message, MessageType
from utils.logger import setup_logger, log_event


class OrchestratorBackup:
    def __init__(
        self,
        host: str = '0.0.0.0',
        client_port: int = 6000,
        worker_port: int = 6001,
        multicast_group: str = '224.1.1.1',
        multicast_port: int = 5007,
        primary_host: str = '127.0.0.1',
        primary_client_port: int = 5000,
        primary_worker_port: int = 5001,
    ):
        # Configurações de rede
        self.host = host
        self.client_port = client_port
        self.worker_port = worker_port
        self.multicast_group = multicast_group
        self.multicast_port = multicast_port

        self.primary_host = primary_host
        self.primary_client_port = primary_client_port
        self.primary_worker_port = primary_worker_port

        # Estado replicado do primário
        self.replicated_state = {
            "tasks": {},
            "workers": {},
            "rr_index": 0,
            "auth": {},
            "cluster": {},
            "lamport_time": 0,
            "timestamp": 0,
        }

        # Estado local
        self.is_primary = False
        self.is_running = True
        self.primary_alive = True
        self.last_primary_heartbeat = time.time()

        # Componentes
        self.clock = LamportClock("orchestrator_backup")
        self.logger = setup_logger("orchestrator_backup")

        # Locks
        self._state_lock = threading.Lock()

        # Controle dos redirecionadores passivos
        self._redirect_threads = []

        log_event(self.logger, self.clock.tick(), "INIT", "Orquestrador backup inicializado")

    def start(self):
        """Inicia serviços do backup."""
        log_event(self.logger, self.clock.tick(), "START", "Iniciando serviços do orquestrador backup")

        multicast_thread = threading.Thread(target=self._receive_multicast, daemon=True)
        multicast_thread.start()

        monitor_thread = threading.Thread(target=self._monitor_primary, daemon=True)
        monitor_thread.start()

        # Enquanto passivo, aceita conexão para orientar clientes/workers ao primário ativo.
        self._start_passive_redirectors()

        log_event(self.logger, self.clock.tick(), "RUNNING", "Backup ativo - aguardando sincronização/failover")

        try:
            while self.is_running:
                time.sleep(1)
                if self.is_primary:
                    self._run_as_primary()
                    break
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        log_event(self.logger, self.clock.tick(), "SHUTDOWN", "Encerrando orquestrador backup")
        self.is_running = False

    # ==================== RECEPÇÃO DE ESTADO ====================

    def _receive_multicast(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self.multicast_port))
        sock.settimeout(2)

        mreq = struct.pack("4sl", socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        log_event(
            self.logger,
            self.clock.tick(),
            "MULTICAST_READY",
            f"Ouvindo multicast em {self.multicast_group}:{self.multicast_port}",
        )

        while self.is_running and not self.is_primary:
            try:
                data, addr = sock.recvfrom(65535)
                msg = Message.from_bytes(data)
                self.clock.receive_event(msg.lamport_time)

                if msg.msg_type == MessageType.STATE_SYNC.value:
                    self._update_state(msg.payload)
                    self.last_primary_heartbeat = time.time()
                    self.primary_alive = True

                    tasks_count = len(msg.payload.get("tasks", {}))
                    workers_count = len(msg.payload.get("workers", {}))
                    sessions_count = len(msg.payload.get("auth", {}).get("tokens", {}))
                    log_event(
                        self.logger,
                        self.clock.get_time(),
                        "STATE_SYNCED",
                        f"Estado sincronizado de {addr} | tarefas={tasks_count} workers={workers_count} sessions={sessions_count}",
                    )

            except socket.timeout:
                continue
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "MULTICAST_ERROR", f"Erro ao receber multicast: {e}")

        sock.close()

    def _update_state(self, state: dict):
        with self._state_lock:
            self.replicated_state = {
                "tasks": state.get("tasks", {}),
                "workers": state.get("workers", {}),
                "rr_index": state.get("rr_index", 0),
                "auth": state.get("auth", {}),
                "cluster": state.get("cluster", {}),
                "lamport_time": state.get("lamport_time", 0),
                "timestamp": state.get("timestamp", 0),
            }

    # ==================== DETECÇÃO DE FALHA ====================

    def _monitor_primary(self):
        failover_timeout = 18

        while self.is_running and not self.is_primary:
            time.sleep(3)
            elapsed = time.time() - self.last_primary_heartbeat

            if elapsed > failover_timeout and self.primary_alive:
                log_event(
                    self.logger,
                    self.clock.tick(),
                    "PRIMARY_FAILURE",
                    f"Primário sem sincronização há {elapsed:.1f}s",
                )
                log_event(self.logger, self.clock.tick(), "FAILOVER_ACTIVATE", "Backup assumindo como primário")
                self.primary_alive = False
                self.is_primary = True
                break

            if elapsed > failover_timeout / 2:
                log_event(
                    self.logger,
                    self.clock.tick(),
                    "PRIMARY_WARNING",
                    f"Sem sincronização do primário há {elapsed:.1f}s (timeout={failover_timeout}s)",
                )

    # ==================== REDIRECIONAMENTO PASSIVO ====================

    def _start_passive_redirectors(self):
        specs = [
            (self.client_port, "client", self.primary_client_port),
            (self.worker_port, "worker", self.primary_worker_port),
        ]
        for listen_port, channel, target_port in specs:
            t = threading.Thread(
                target=self._run_redirect_server,
                args=(listen_port, channel, self.primary_host, target_port),
                daemon=True,
            )
            t.start()
            self._redirect_threads.append(t)

    def _run_redirect_server(self, listen_port: int, channel: str, target_host: str, target_port: int):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((self.host, listen_port))
            sock.listen(20)
            sock.settimeout(2)
            log_event(
                self.logger,
                self.clock.tick(),
                "REDIRECT_READY",
                f"Redirecionador passivo em {self.host}:{listen_port} -> {target_host}:{target_port} ({channel})",
            )
        except Exception as e:
            log_event(
                self.logger,
                self.clock.tick(),
                "REDIRECT_DISABLED",
                f"Não foi possível subir redirecionador em {listen_port}: {e}",
            )
            sock.close()
            return

        while self.is_running and not self.is_primary:
            try:
                conn, addr = sock.accept()
                try:
                    redirect = Message(
                        msg_type=MessageType.REDIRECT.value,
                        sender_id="orchestrator_backup",
                        payload={
                            "channel": channel,
                            "host": target_host,
                            "port": target_port,
                            "reason": "backup passivo",
                        },
                        lamport_time=self.clock.send_event(),
                    )
                    self._send_message(conn, redirect.to_json())
                    log_event(
                        self.logger,
                        self.clock.get_time(),
                        "REDIRECT_SENT",
                        f"{channel} {addr} redirecionado para {target_host}:{target_port}",
                    )
                finally:
                    conn.close()
            except socket.timeout:
                continue
            except Exception as e:
                log_event(self.logger, self.clock.tick(), "REDIRECT_ERROR", f"Erro no redirecionador {channel}: {e}")

        sock.close()

    # ==================== FAILOVER ====================

    def _run_as_primary(self):
        from orchestrator.orchestrator import Orchestrator

        chosen_client_port, chosen_worker_port = self._select_failover_ports()
        log_event(
            self.logger,
            self.clock.tick(),
            "FAILOVER_START",
            f"Iniciando orquestrador ativo em {self.host}:{chosen_client_port}/{chosen_worker_port}",
        )

        new_orchestrator = Orchestrator(
            host=self.host,
            client_port=chosen_client_port,
            worker_port=chosen_worker_port,
            multicast_group=self.multicast_group,
            multicast_port=self.multicast_port,
            node_id="orchestrator_backup_primary",
            role="failover-primary",
        )

        with self._state_lock:
            new_orchestrator.restore_replicated_state(self.replicated_state)

        log_event(
            self.logger,
            self.clock.tick(),
            "FAILOVER_COMPLETE",
            (
                f"Failover concluído | endpoint clientes={self.host}:{chosen_client_port} "
                f"endpoint workers={self.host}:{chosen_worker_port}"
            ),
        )

        # O próprio orquestrador ativo continuará publicando estado atualizado no multicast.
        new_orchestrator.start()

    def _select_failover_ports(self) -> Tuple[int, int]:
        preferred = (self.primary_client_port, self.primary_worker_port)
        fallback = (self.client_port, self.worker_port)

        if self._can_bind_pair(preferred[0], preferred[1]):
            log_event(
                self.logger,
                self.clock.tick(),
                "FAILOVER_PORTS_PRIMARY",
                f"Assumindo portas do primário: {preferred[0]}/{preferred[1]}",
            )
            return preferred

        log_event(
            self.logger,
            self.clock.tick(),
            "FAILOVER_PORTS_FALLBACK",
            (
                "Não foi possível assumir portas do primário. "
                f"Usando fallback {fallback[0]}/{fallback[1]} (redirecionamento automático por endpoint secundário)"
            ),
        )
        return fallback

    def _can_bind_pair(self, client_port: int, worker_port: int) -> bool:
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s1.bind((self.host, client_port))
            s2.bind((self.host, worker_port))
            return True
        except OSError:
            return False
        finally:
            s1.close()
            s2.close()

    @staticmethod
    def _send_message(conn: socket.socket, data: str):
        encoded = data.encode('utf-8')
        conn.sendall(struct.pack('!I', len(encoded)) + encoded)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Orquestrador Backup')
    parser.add_argument('--host', default='0.0.0.0', help='Host do servidor')
    parser.add_argument('--client-port', type=int, default=6000, help='Porta passiva para clientes')
    parser.add_argument('--worker-port', type=int, default=6001, help='Porta passiva para workers')
    parser.add_argument('--multicast-group', default='224.1.1.1', help='Grupo multicast')
    parser.add_argument('--multicast-port', type=int, default=5007, help='Porta multicast')
    parser.add_argument('--primary-host', default='127.0.0.1', help='Host do primário para redirecionamento')
    parser.add_argument('--primary-client-port', type=int, default=5000, help='Porta de clientes do primário')
    parser.add_argument('--primary-worker-port', type=int, default=5001, help='Porta de workers do primário')

    args = parser.parse_args()

    backup = OrchestratorBackup(
        host=args.host,
        client_port=args.client_port,
        worker_port=args.worker_port,
        multicast_group=args.multicast_group,
        multicast_port=args.multicast_port,
        primary_host=args.primary_host,
        primary_client_port=args.primary_client_port,
        primary_worker_port=args.primary_worker_port,
    )
    backup.start()
