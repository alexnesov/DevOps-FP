import logging
import os
from datetime import datetime

current_time = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
filename = f"process_{current_time}.log"
log_file_name = os.path.join("utils/logs", filename)
# Set up logging configuration
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file_name,
        encoding='utf-8'
    )
except Exception as e:
    print(f"Error occurred while setting up logging: {str(e)}")
    exit(1)

# Create a logger object
logger = logging.getLogger(__name__)

# Example function to log a message


def log_message(message):
    logger.info(message)
    print(message)


if __name__ == '__main__':
    pass
