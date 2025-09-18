#!/usr/bin/env python3
"""Тестовий запуск міграції без збереження даних"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.migrators.bld_local import BldLocalMigrator
from src.migrators.rtg_addr import RtgAddrMigrator  
from src.migrators.ek_addr import EkAddrMigrator

def test_dry_run():
    """Тестовий запуск всіх міграторів"""
    
    print("🧪 ТЕСТОВИЙ ЗАПУСК МІГРАЦІЇ (без збереження)")
    print("=" * 50)
    
    # Тест bld_local
    print("🔍 Тест bld_local мігратора...")
    try:
        migrator = BldLocalMigrator()
        migrator.migrate(dry_run=True, batch_size=10)
        print("✅ bld_local мігратор працює")
    except Exception as e:
        print(f"❌ bld_local мігратор помилка: {e}")
    
    # Тест rtg_addr
    print("\n🔍 Тест rtg_addr мігратора...")
    try:
        migrator = RtgAddrMigrator()
        migrator.migrate(dry_run=True, batch_size=10)
        print("✅ rtg_addr мігратор працює")
    except Exception as e:
        print(f"❌ rtg_addr мігратор помилка: {e}")
    
    # Тест ek_addr
    print("\n🔍 Тест ek_addr мігратора...")
    try:
        migrator = EkAddrMigrator()
        migrator.migrate(dry_run=True, batch_size=10)
        print("✅ ek_addr мігратор працює")
    except Exception as e:
        print(f"❌ ek_addr мігратор помилка: {e}")

if __name__ == "__main__":
    test_dry_run()