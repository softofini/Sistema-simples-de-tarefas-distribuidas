"""
Logger centralizado do sistema distribuído.
Registra todos os eventos relevantes com timestamps Lamport.
"""

import logging
import os
import sys
from datetime import datetime


def setup_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Configura um logger que escreve em arquivo e no console.
    
    Args:
        name: Nome do componente (ex: 'orchestrator', 'worker_1')
        log_dir: Diretório para salvar os arquivos de log
    
    Returns:
        Logger configurado
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Evitar duplicação de handlers
    if logger.handlers:
        return logger

    # Formato do log
    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler para arquivo
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"{name}.log"),
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def log_event(logger: logging.Logger, lamport_time: int, event: str, details: str = ""):
    """
    Registra um evento com timestamp Lamport.
    
    Args:
        logger: Logger do componente
        lamport_time: Timestamp do relógio de Lamport
        event: Tipo do evento
        details: Detalhes adicionais
    """
    msg = f"[Lamport={lamport_time}] {event}"
    if details:
        msg += f" | {details}"
    logger.info(msg)
