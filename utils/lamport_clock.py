"""
Relógio Lógico de Lamport
Implementa a ordenação parcial de eventos em sistemas distribuídos.
"""

import threading


class LamportClock:
    """
    Implementação do Relógio Lógico de Lamport.
    
    Regras:
    1. Antes de cada evento local, incrementar o relógio.
    2. Ao enviar uma mensagem, incluir o timestamp do relógio.
    3. Ao receber uma mensagem, ajustar o relógio para max(local, recebido) + 1.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.time = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Evento local: incrementa o relógio."""
        with self._lock:
            self.time += 1
            return self.time

    def send_event(self) -> int:
        """Evento de envio: incrementa e retorna o timestamp para a mensagem."""
        with self._lock:
            self.time += 1
            return self.time

    def receive_event(self, received_time: int) -> int:
        """Evento de recebimento: ajusta o relógio com base no timestamp recebido."""
        with self._lock:
            self.time = max(self.time, received_time) + 1
            return self.time

    def get_time(self) -> int:
        """Retorna o timestamp atual sem incrementar."""
        with self._lock:
            return self.time

    def __repr__(self):
        return f"LamportClock(node={self.node_id}, time={self.time})"
