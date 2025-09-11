#!/bin/bash

# Скрипт створення структури проекту addrinity-migration

echo "Створення структури проекту addrinity-migration..."

# Створення основних директорій
mkdir -p config
mkdir -p src/migrators
mkdir -p src/processors
mkdir -p src/utils
mkdir -p src/ai_helper
mkdir -p logs
mkdir -p migrations/reports

# Створення порожніх файлів Python
touch config/__init__.py
touch config/database.py

touch src/__init__.py
touch src/migrators/__init__.py
touch src/migrators/bld_local.py
touch src/migrators/ek_addr.py
touch src/migrators/rtg_addr.py
touch src/processors/__init__.py
touch src/processors/address_parser.py
touch src/utils/__init__.py
touch src/utils/validators.py
touch src/utils/logger.py
touch src/ai_helper/__init__.py
touch src/ai_helper/fuzzy_search.py
touch src/ai_helper/address_parser_ai.py

# Створення основних файлів
touch requirements.txt
touch migrate.py
touch search_api.py
touch README.md

echo "Структура проекту створена успішно!"
echo "Для встановлення залежностей запустіть: ./setup/install_dependencies.sh"