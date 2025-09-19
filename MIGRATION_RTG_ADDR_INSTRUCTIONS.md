# Інструкція по запуску рефакторованої міграції RTG_ADDR

## Огляд змін
Повністю рефакторований мігратор з наступними покращеннями:
- ✅ Ідемпотентні INSERT ... ON CONFLICT операції
- ✅ Нормалізація назв та типів
- ✅ Збереження оригінальних даних у JSONB
- ✅ Обробка edge cases (NULL, пусті значення)
- ✅ Детальне логування та статистика
- ✅ Підтримка DRY RUN режиму
- ✅ Читання даних з файлу migrations/DATA-TrinitY-3.txt

## Встановлення залежностей
```bash
pip install psycopg2-binary tqdm
```

## Використання через основний скрипт migrate.py
```bash
# Тестовий запуск
python migrate.py --tables rtg_addr --dry-run --batch-size 50

# Повна міграція
python migrate.py --tables rtg_addr --batch-size 1000
```

## Прямий запуск
```python
from src.migrators.rtg_addr import RtgAddrMigrator
from config.database import CONNECTION_STRING

# Тестовий запуск
migrator = RtgAddrMigrator(CONNECTION_STRING)
migrator.migrate(dry_run=True, batch_size=50)

# Повна міграція
migrator.migrate(dry_run=False, batch_size=1000)
```

## Особливості реалізації
- Читає дані з файлу migrations/DATA-TrinitY-3.txt
- Підтримує роботу без БД (DRY RUN режим)
- Кешування для мінімізації запитів до БД
- Повна зворотна сумісність з існуючим кодом

## Логи
Логи зберігаються в logs/migration.log з детальною статистикою.
