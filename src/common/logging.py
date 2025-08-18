import structlog

def get_structured_logger(name: str):
    return structlog.get_logger(name)
