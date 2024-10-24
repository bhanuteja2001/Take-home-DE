import logging
import os

def setup_logging(log_file='logs/processing.log'):
    # Ensure the logs directory exists in the project root
    os.makedirs(os.path.join(os.path.dirname(__file__), '..', 'logs'), exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(os.path.dirname(__file__), '..', log_file),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def log_issue(user_id, issue):
    logging.error(f"User ID: {user_id}, Issue: {issue}")