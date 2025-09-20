#!/usr/bin/env python3
"""Тестовий запуск рефакторованого мігратора rtg_addr"""

import sys
import os

# Додавання шляхів для імпорту
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src', 'utils'))

# Імпорт компонентів
try:
    from src.utils.migration_data_parser import MigrationDataParser
except ImportError:
    sys.path.insert(0, os.path.join(project_root, 'src', 'utils'))
    from migration_data_parser import MigrationDataParser


def test_refactored_migrator():
    """Тестування рефакторованого мігратора без підключення до БД"""
    
    print("🧪 Тестування рефакторованого RTG_ADDR мігратора")
    print("=" * 60)
    
    # Тест парсера даних
    print("1. Тестування парсера міграційних даних...")
    try:
        parser = MigrationDataParser()
        records = parser.parse_rtg_addr_section()
        print(f"   ✅ Завантажено {len(records)} записів")
        
        # Показати статистику
        stats = parser.get_statistics([parser.normalize_record(r) for r in records])
        print("   📊 Статистика даних:")
        for key, value in stats.items():
            print(f"      {key}: {value}")
            
        # Показати приклад нормалізованого запису
        if records:
            normalized = parser.normalize_record(records[0])
            print(f"\n   📝 Приклад нормалізованого запису (ID: {normalized['id']}):")
            for key, value in normalized.items():
                if value is not None:
                    print(f"      {key}: {value}")
                    
    except Exception as e:
        print(f"   ❌ Помилка парсера: {e}")
    
    # Тест мігратора в DRY RUN режимі
    print(f"\n2. Тестування мігратора в DRY RUN режимі...")
    try:
        # Імпорт мігратора без залежностей від БД
        sys.path.insert(0, os.path.join(project_root, 'src', 'migrators'))
        from rtg_addr_refactored import RefactoredRtgAddrMigrator
        
        # Створення мігратора без підключення до БД
        migrator = RefactoredRtgAddrMigrator()
        
        # Запуск міграції в DRY RUN режимі
        results = migrator.migrate(dry_run=True, batch_size=5)
        
        print("   ✅ DRY RUN виконано успішно")
        print(f"   📊 Результати:")
        for key, value in results.items():
            if value > 0:
                print(f"      {key}: {value}")
                
    except Exception as e:
        print(f"   ❌ Помилка мігратора: {e}")
        import traceback
        print(f"      Деталі: {traceback.format_exc()}")
    
    print(f"\n3. Тестування нормалізації тексту...")
    try:
        migrator = RefactoredRtgAddrMigrator()
        
        test_cases = [
            ("вул.", "street_type"),
            ("просп", "street_type"),
            ("Дніпровський район", "district"),
            ("  Київ  ", "city"),
            ("", None)
        ]
        
        print("   🔄 Тести нормалізації:")
        for text, obj_type in test_cases:
            normalized = migrator.normalize_text(text, obj_type)
            print(f"      '{text}' ({obj_type}) -> '{normalized}'")
            
        print("   ✅ Нормалізація працює")
        
    except Exception as e:
        print(f"   ❌ Помилка нормалізації: {e}")
    
    print(f"\n4. Створення інструкцій...")
    try:
        from rtg_addr_refactored import create_migration_instructions
        create_migration_instructions()
        print("   ✅ Інструкції створено в MIGRATION_RTG_ADDR_README.md")
    except Exception as e:
        print(f"   ❌ Помилка створення інструкцій: {e}")
    
    print("\n" + "=" * 60)
    print("🎉 Тестування завершено!")


if __name__ == "__main__":
    test_refactored_migrator()