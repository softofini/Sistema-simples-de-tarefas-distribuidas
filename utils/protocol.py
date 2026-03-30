"""
Protocolo de comunicação do sistema distribuído.
Define o formato de mensagens trocadas entre todos os componentes.
"""

import json
import hashlib
import secrets
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any


class MessageType(Enum):
    # Autenticação
    AUTH_REQUEST = "AUTH_REQUEST"
    AUTH_RESPONSE = "AUTH_RESPONSE"

    # Tarefas
    TASK_SUBMIT = "TASK_SUBMIT"
    TASK_SUBMIT_ACK = "TASK_SUBMIT_ACK"
    TASK_ASSIGN = "TASK_ASSIGN"
    TASK_ASSIGN_ACK = "TASK_ASSIGN_ACK"
    TASK_STATUS_REQUEST = "TASK_STATUS_REQUEST"
    TASK_STATUS_RESPONSE = "TASK_STATUS_RESPONSE"
    TASK_COMPLETE = "TASK_COMPLETE"
    TASK_FAILED = "TASK_FAILED"
    TASK_REASSIGN = "TASK_REASSIGN"

    # Heartbeat
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_ACK = "HEARTBEAT_ACK"

    # Sincronização Orquestrador ↔ Backup
    STATE_SYNC = "STATE_SYNC"
    STATE_SYNC_ACK = "STATE_SYNC_ACK"
    FAILOVER_ACTIVATE = "FAILOVER_ACTIVATE"

    # Worker
    WORKER_REGISTER = "WORKER_REGISTER"
    WORKER_REGISTER_ACK = "WORKER_REGISTER_ACK"
    WORKER_STATUS = "WORKER_STATUS"

    # Simulação de falha
    SIMULATE_FAILURE = "SIMULATE_FAILURE"


class TaskStatus(Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REASSIGNED = "REASSIGNED"


@dataclass
class Message:
    msg_type: str
    sender_id: str
    payload: Dict[str, Any] = field(default_factory=dict)
    lamport_time: int = 0
    token: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> 'Message':
        d = json.loads(data)
        return cls(**d)

    def to_bytes(self) -> bytes:
        return self.to_json().encode('utf-8')

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Message':
        return cls.from_json(data.decode('utf-8'))


@dataclass
class Task:
    task_id: str
    client_id: str
    description: str
    status: str = TaskStatus.PENDING.value
    assigned_worker: Optional[str] = None
    result: Optional[str] = None
    created_at: float = 0.0
    completed_at: float = 0.0
    lamport_created: int = 0
    lamport_completed: int = 0
    retries: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'Task':
        return cls(**d)


class AuthManager:
    """Gerenciador de autenticação simples com tokens."""

    def __init__(self):
        # Banco de usuários simulado (usuário: hash da senha)
        self.users: Dict[str, str] = {}
        self.tokens: Dict[str, str] = {}  # token -> username
        self._lock = threading.Lock()

        # Registrar usuários padrão
        self.register_user("admin", "admin123")
        self.register_user("usuario1", "senha123")
        self.register_user("usuario2", "senha456")
        self.register_user("usuario3", "senha789")

    def register_user(self, username: str, password: str):
        pw_hash = hashlib.sha256(password.encode()).hexdigest()
        self.users[username] = pw_hash

    def authenticate(self, username: str, password: str) -> Optional[str]:
        """Autentica o usuário e retorna um token ou None."""
        pw_hash = hashlib.sha256(password.encode()).hexdigest()
        if username in self.users and self.users[username] == pw_hash:
            token = secrets.token_hex(16)
            with self._lock:
                self.tokens[token] = username
            return token
        return None

    def validate_token(self, token: str) -> Optional[str]:
        """Valida um token e retorna o username ou None."""
        with self._lock:
            return self.tokens.get(token)

    def revoke_token(self, token: str):
        with self._lock:
            self.tokens.pop(token, None)


import threading
