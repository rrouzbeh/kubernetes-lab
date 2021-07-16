import logging
from config.config import settings


log_level = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
print(settings.get('LOG_LEVEL'))
logging.basicConfig(format='%(asctime)s - %(message)s',
                    level=log_level[settings.get('log_level')])

logger = logging.getLogger("output")