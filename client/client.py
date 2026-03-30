"""
Cliente da Plataforma Distribuída
====================================
Responsabilidades:
- Autenticar-se no orquestrador
- Submeter tarefas
- Consultar status de tarefas em tempo real
"""

import socket
import struct
import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.lamport_clock import LamportClock
from utils.protocol import Message, MessageType
from utils.logger import setup_logger, log_event


class Client:
    def __init__(self, client_id: str, orchestrator_host='127.0.0.1',
                 orchestrator_port=5000, fallback_endpoints=None):
        self.client_id = client_id
        self.orchestrator_host = orchestrator_host
        self.orchestrator_port = orchestrator_port
        self.fallback_endpoints = fallback_endpoints or []
        self.token = None
        self.conn = None
        self._username = None
        self._password = None

        self.clock = LamportClock(client_id)
        self.logger = setup_logger(client_id)

        log_event(self.logger, self.clock.tick(), "INIT",
                  f"Cliente '{client_id}' inicializado")

    def _do_connect(self, host: str, port: int) -> bool:
        """Tenta conectar a um endpoint específico."""
        try:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect((host, port))
            return True
        except (ConnectionRefusedError, OSError):
            return False

    def connect(self):
        """Conecta ao orquestrador."""
        if self._do_connect(self.orchestrator_host, self.orchestrator_port):
            log_event(self.logger, self.clock.tick(), "CONNECTED",
                      f"Conectado ao orquestrador em {self.orchestrator_host}:{self.orchestrator_port}")
            return True
        log_event(self.logger, self.clock.tick(), "CONNECTION_FAILED",
                  "Não foi possível conectar ao orquestrador")
        return False

    def _try_reconnect(self, timeout_seconds: int = 20, retry_interval: float = 1.0) -> bool:
        """Tenta reconectar por alguns segundos para cobrir a janela de failover do backup."""
        # Remove duplicados preservando ordem
        endpoints = []
        for endpoint in [(self.orchestrator_host, self.orchestrator_port)] + self.fallback_endpoints:
            if endpoint not in endpoints:
                endpoints.append(endpoint)

        start = time.time()
        while (time.time() - start) < timeout_seconds:
            for host, port in endpoints:
                if self._do_connect(host, port):
                    self.orchestrator_host = host
                    self.orchestrator_port = port
                    log_event(self.logger, self.clock.tick(), "RECONNECTED",
                              f"Reconectado ao orquestrador em {host}:{port}")
                    print(f"\n[!] Reconectado ao orquestrador em {host}:{port}")
                    if self._username and self._password:
                        return self.authenticate(self._username, self._password)
                    return True
            time.sleep(retry_interval)

        log_event(self.logger, self.clock.tick(), "RECONNECT_FAILED",
                  f"Não foi possível reconectar após {timeout_seconds}s")
        return False

    def disconnect(self):
        """Desconecta do orquestrador."""
        if self.conn:
            self.conn.close()
            log_event(self.logger, self.clock.tick(), "DISCONNECTED",
                      "Desconectado do orquestrador")

    def authenticate(self, username: str, password: str) -> bool:
        """
        Autentica o cliente no orquestrador.
        Retorna True se a autenticação foi bem-sucedida.
        """
        self._username = username
        self._password = password
        msg = Message(
            msg_type=MessageType.AUTH_REQUEST.value,
            sender_id=self.client_id,
            payload={"username": username, "password": password},
            lamport_time=self.clock.send_event()
        )
        self._send_message(msg.to_json())

        # Receber resposta
        data = self._recv_message()
        if not data:
            return False

        response = Message.from_json(data)
        self.clock.receive_event(response.lamport_time)

        if response.payload.get("success"):
            self.token = response.payload.get("token")
            log_event(self.logger, self.clock.get_time(), "AUTH_SUCCESS",
                      f"Autenticação bem-sucedida como '{username}'")
            return True
        else:
            log_event(self.logger, self.clock.get_time(), "AUTH_FAILED",
                      f"Falha na autenticação: {response.payload.get('message')}")
            return False

    def submit_task(self, description: str) -> dict:
        """
        Submete uma tarefa ao orquestrador.
        Retorna informações da tarefa ou None em caso de erro.
        """
        if not self.token:
            log_event(self.logger, self.clock.tick(), "ERROR",
                      "Não autenticado. Faça login primeiro.")
            return {"success": False, "message": "Não autenticado"}

        msg = Message(
            msg_type=MessageType.TASK_SUBMIT.value,
            sender_id=self.client_id,
            payload={"description": description},
            lamport_time=self.clock.send_event(),
            token=self.token
        )
        data = self._request(msg)
        if not data:
            return {"success": False, "message": "Orquestrador inacessível"}

        response = Message.from_json(data)
        self.clock.receive_event(response.lamport_time)

        log_event(self.logger, self.clock.get_time(), "TASK_SUBMITTED",
                  f"Tarefa submetida: {response.payload}")

        return response.payload

    def get_task_status(self, task_id: str = None) -> dict:
        """
        Consulta o status de tarefas.
        Se task_id for fornecido, retorna status de uma tarefa específica.
        Caso contrário, retorna todas as tarefas do usuário.
        """
        if not self.token:
            return {"success": False, "message": "Não autenticado"}

        msg = Message(
            msg_type=MessageType.TASK_STATUS_REQUEST.value,
            sender_id=self.client_id,
            payload={"task_id": task_id} if task_id else {},
            lamport_time=self.clock.send_event(),
            token=self.token
        )
        data = self._request(msg)
        if not data:
            return {"success": False, "message": "Orquestrador inacessível"}

        response = Message.from_json(data)
        self.clock.receive_event(response.lamport_time)

        return response.payload

    # ==================== COMUNICAÇÃO TCP ====================

    def _request(self, msg) -> str:
        """Envia uma mensagem e retorna a resposta bruta, com reconexão automática em caso de falha."""
        if not self._send_message(msg.to_json()):
            if not self._try_reconnect():
                return None
            msg.token = self.token
            if not self._send_message(msg.to_json()):
                return None
        data = self._recv_message()
        if data is None:
            if not self._try_reconnect():
                return None
            msg.token = self.token
            if not self._send_message(msg.to_json()):
                return None
            data = self._recv_message()
        return data

    def _send_message(self, data: str) -> bool:
        try:
            encoded = data.encode('utf-8')
            length = len(encoded)
            self.conn.sendall(struct.pack('!I', length) + encoded)
            return True
        except (OSError, BrokenPipeError, ConnectionResetError):
            return False

    def _recv_message(self) -> str:
        raw_length = self._recv_exact(4)
        if not raw_length:
            return None
        length = struct.unpack('!I', raw_length)[0]

        raw_data = self._recv_exact(length)
        if not raw_data:
            return None
        return raw_data.decode('utf-8')

    def _recv_exact(self, n: int) -> bytes:
        data = b''
        while len(data) < n:
            chunk = self.conn.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data


def interactive_client():
    """Interface interativa para o cliente."""
    print("=" * 60)
    print(" PLATAFORMA DISTRIBUÍDA DE PROCESSAMENTO COLABORATIVO")
    print(" Cliente Interativo")
    print("=" * 60)

    host = input("\nHost do orquestrador [127.0.0.1]: ").strip() or "127.0.0.1"
    port = input("Porta do orquestrador [5000]: ").strip() or "5000"
    backup_host = input("Host do orquestrador backup [127.0.0.1]: ").strip() or "127.0.0.1"
    backup_port = input("Porta do backup [6000]: ").strip() or "6000"

    client = Client(
        client_id=f"client_{int(time.time()) % 10000}",
        orchestrator_host=host,
        orchestrator_port=int(port),
        fallback_endpoints=[(backup_host, int(backup_port))]
    )

    if not client.connect():
        print("Erro: Não foi possível conectar ao orquestrador.")
        return

    # Autenticação
    print("\n--- Autenticação ---")
    print("Usuários disponíveis: admin/admin123, usuario1/senha123, usuario2/senha456")
    username = input("Usuário: ").strip()
    password = input("Senha: ").strip()

    if not client.authenticate(username, password):
        print("Erro: Autenticação falhou.")
        client.disconnect()
        return

    print(f"\n✓ Autenticado como '{username}'\n")

    # Menu interativo
    while True:
        print("\n--- Menu ---")
        print("1. Submeter tarefa")
        print("2. Consultar status (todas as tarefas)")
        print("3. Consultar status (tarefa específica)")
        print("4. Submeter múltiplas tarefas")
        print("5. Sair")
        print("-" * 30)

        choice = input("Opção: ").strip()

        if choice == "1":
            desc = input("Descrição da tarefa: ").strip()
            if desc:
                result = client.submit_task(desc)
                print(f"\n→ Resposta: {json.dumps(result, indent=2, ensure_ascii=False)}")

        elif choice == "2":
            result = client.get_task_status()
            if result.get("success"):
                tasks = result.get("tasks", [])
                if tasks:
                    print(f"\n{'ID':<12} {'Status':<12} {'Worker':<12} {'Descrição'}")
                    print("-" * 60)
                    for t in tasks:
                        print(f"{t['task_id']:<12} {t['status']:<12} "
                              f"{t.get('assigned_worker', 'N/A'):<12} {t['description']}")
                else:
                    print("\nNenhuma tarefa encontrada.")
            else:
                print(f"\nErro: {result.get('message')}")

        elif choice == "3":
            task_id = input("ID da tarefa: ").strip()
            result = client.get_task_status(task_id)
            print(f"\n→ Resposta: {json.dumps(result, indent=2, ensure_ascii=False)}")

        elif choice == "4":
            n = int(input("Quantas tarefas? ").strip() or "5")
            for i in range(n):
                desc = f"Tarefa automática #{i + 1} - Processamento de dados"
                result = client.submit_task(desc)
                print(f"  Tarefa {i + 1}: {result.get('task_id', 'erro')} - {result.get('status', 'erro')}")
                time.sleep(0.3)
            print(f"\n✓ {n} tarefas submetidas!")

        elif choice == "5":
            break
        else:
            print("Opção inválida.")

    client.disconnect()
    print("\nCliente encerrado.")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Cliente')
    parser.add_argument('--host', default='127.0.0.1', help='Host do orquestrador')
    parser.add_argument('--port', type=int, default=5000, help='Porta do orquestrador')
    parser.add_argument('--interactive', action='store_true', help='Modo interativo')

    args = parser.parse_args()

    if args.interactive:
        interactive_client()
    else:
        interactive_client()
