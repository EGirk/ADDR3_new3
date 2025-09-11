#!/usr/bin/env python3
"""
Головний скрипт міграції даних до addrinity
"""

import argparse
import sys
from src.utils.logger import migration_logger
from src.migrators.bld_local import BldLocalMigrator
from src.migrators.ek_addr import EkAddrMigrator
from src.migrators.rtg_addr import RtgAddrMigrator

def main():
    parser = argparse.ArgumentParser(description='Міграція даних до addrinity')
    parser.add_argument('--tables', nargs='+', 
                       choices=['bld_local', 'ek_addr', 'rtg_addr', 'all'],
                       default=['all'],
                       help='Які таблиці мігрувати')
    parser.add_argument('--dry-run', action='store_true',
                       help='Тестовий запуск без збереження даних')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Розмір батчу для обробки')
    
    args = parser.parse_args()
    
    try:
        migration_logger.info("Початок міграції addrinity")
        
        tables_to_migrate = args.tables
        if 'all' in tables_to_migrate:
            tables_to_migrate = ['bld_local', 'ek_addr', 'rtg_addr']
        
        # Міграція обраних таблиць
        if 'bld_local' in tables_to_migrate:
            migrator = BldLocalMigrator()
            migrator.migrate(dry_run=args.dry_run, batch_size=args.batch_size)
        
        if 'ek_addr' in tables_to_migrate:
            migrator = EkAddrMigrator()
            migrator.migrate(dry_run=args.dry_run, batch_size=args.batch_size)
        
        if 'rtg_addr' in tables_to_migrate:
            migrator = RtgAddrMigrator()
            migrator.migrate(dry_run=args.dry_run, batch_size=args.batch_size)
        
        migration_logger.info("Міграція завершена успішно!")
        
    except Exception as e:
        migration_logger.error(f"Помилка міграції: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()