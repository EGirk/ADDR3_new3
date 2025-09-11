@echo off
echo Створення структури проекту addrinity-migration...

REM Створення основних директорій
mkdir config
mkdir src\migrators
mkdir src\processors
mkdir src\utils
mkdir src\ai_helper
mkdir logs
mkdir migrations\reports

REM Створення порожніх файлів Python
type nul > config\__init__.py
type nul > config\database.py

type nul > src\__init__.py
type nul > src\migrators\__init__.py
type nul > src\migrators\bld_local.py
type nul > src\migrators\ek_addr.py
type nul > src\migrators\rtg_addr.py
type nul > src\processors\__init__.py
type nul > src\processors\address_parser.py
type nul > src\utils\__init__.py
type nul > src\utils\validators.py
type nul > src\utils\logger.py
type nul > src\ai_helper\__init__.py
type nul > src\ai_helper\fuzzy_search.py
type nul > src\ai_helper\address_parser_ai.py

REM Створення основних файлів
type nul > requirements.txt
type nul > migrate.py
type nul > search_api.py
type nul > README.md

echo Структура проекту створена успішно!
echo Для встановлення залежностей запустіть: setup\install_dependencies.bat