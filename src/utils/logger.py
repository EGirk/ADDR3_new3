"""Налаштування логування для проекту"""

import logging
import sys
from datetime import datetime

def setup_logger(name: str, log_file: str = None, level=logging.INFO):
    """Налаштування логера"""
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Консольний handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Файловий handler (якщо вказано файл)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Основний логер для проекту
migration_logger = setup_logger('AddrinityMigration', 'logs/migration.log')