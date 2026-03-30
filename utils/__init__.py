# Pacote utils
from utils.lamport_clock import LamportClock
from utils.protocol import (
    Message, MessageType, Task, TaskStatus, AuthManager
)
from utils.logger import setup_logger, log_event
