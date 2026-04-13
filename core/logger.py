"""Structured logging — UTF-8 safe stream handler."""
import logging
import sys

# Use a UTF-8 stream writer so Windows cp949 console doesn't choke on em dashes
_stream = open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1, closefd=False)

_handler = logging.StreamHandler(_stream)
_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s  %(filename)s:%(lineno)d", datefmt="%H:%M:%S"))

logging.basicConfig(level=logging.INFO, handlers=[_handler])

# Suppress noisy httpx request logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

log = logging.getLogger("prediction_edge")
