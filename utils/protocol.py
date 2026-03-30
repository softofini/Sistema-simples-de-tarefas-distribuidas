"""
Protocolo de comunicação do sistema distribuído.
Define o formato de mensagens trocadas entre todos os componentes.
"""

import json
import hashlib
import secrets
import threading
import time
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
    TASK_STARTED = "TASK_STARTED"
    TASK_STATUS_REQUEST = "TASK_STATUS_REQUEST"
    TASK_STATUS_RESPONSE = "TASK_STATUS_RESPONSE"
    TASK_COMPLETE = "TASK_COMPLETE"
    TASK_FAILED = "TASK_FAILED"
    REDIRECT = "REDIRECT"

    # Heartbeat
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_ACK = "HEARTBEAT_ACK"
    ORCHESTRATOR_HEARTBEAT = "ORCHESTRATOR_HEARTBEAT"

    # Sincronização Orquestrador ↔ Backup
    STATE_SYNC = "STATE_SYNC"
    FAILOVER_ACTIVATE = "FAILOVER_ACTIVATE"

    # Worker
    WORKER_REGISTER = "WORKER_REGISTER"
    WORKER_REGISTER_ACK = "WORKER_REGISTER_ACK"

    # Simulação de falha
    SIMULATE_FAILURE = "SIMULATE_FAILURE"


class TaskStatus(Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


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
    first_started_worker: Optional[str] = None
    completed_worker: Optional[str] = None
    failed_workers: list = field(default_factory=list)
    result: Optional[str] = None
    last_error: Optional[str] = None
    created_at: float = 0.0
    assigned_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    lamport_created: int = 0
    lamport_assigned: int = 0
    lamport_started: int = 0
    lamport_completed: int = 0
    last_updated_lamport: int = 0
    retries: int = 0
    # Histórico completo de execução: lista de tentativas com worker, timestamps e Lamport time
    execution_history: list = field(default_factory=list)  # [{"worker_id": str, "event": str, "timestamp": float, "lamport_time": int}]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'Task':
        return cls(**d)


class AuthManager:
    """Gerenciador de autenticação com sessões replicáveis."""

    def __init__(self, token_ttl_seconds: int = 3600):
        # Banco de usuários simulado (usuário: hash da senha)
        self.users: Dict[str, str] = {}
        self.tokens: Dict[str, Dict[str, Any]] = {}
        self.token_ttl_seconds = token_ttl_seconds
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
            now = time.time()
            with self._lock:
                self.tokens[token] = {
                    "username": username,
                    "issued_at": now,
                    "last_seen": now,
                    "expires_at": now + self.token_ttl_seconds,
                }
            return token
        return None

    def validate_token(self, token: str) -> Optional[str]:
        """Valida um token e retorna o username ou None."""
        with self._lock:
            session = self.tokens.get(token)
            if not session:
                return None

            now = time.time()
            if now >= float(session.get("expires_at", 0)):
                self.tokens.pop(token, None)
                return None

            session["last_seen"] = now
            return session.get("username")

    def touch_token(self, token: str):
        """Atualiza metadados de uso da sessão sem alterar validade."""
        with self._lock:
            session = self.tokens.get(token)
            if session:
                session["last_seen"] = time.time()

    def revoke_token(self, token: str):
        with self._lock:
            self.tokens.pop(token, None)

    def export_state(self) -> Dict[str, Any]:
        """Serializa sessões autenticadas para replicação."""
        with self._lock:
            return {
                "tokens": {k: dict(v) for k, v in self.tokens.items()},
                "token_ttl_seconds": self.token_ttl_seconds,
            }

    def import_state(self, state: Dict[str, Any]):
        """Restaura sessões autenticadas replicadas do primário."""
        tokens = state.get("tokens", {}) if state else {}
        ttl = state.get("token_ttl_seconds", self.token_ttl_seconds) if state else self.token_ttl_seconds
        with self._lock:
            self.token_ttl_seconds = int(ttl)
            self.tokens = {}
            for token, session in tokens.items():
                self.tokens[token] = {
                    "username": session.get("username"),
                    "issued_at": float(session.get("issued_at", time.time())),
                    "last_seen": float(session.get("last_seen", time.time())),
                    "expires_at": float(session.get("expires_at", time.time() + self.token_ttl_seconds)),
                }
