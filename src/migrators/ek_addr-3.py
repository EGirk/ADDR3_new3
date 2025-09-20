"""Повний мігратор для addr.ek_addr з універсальним валідатором"""

import pandas as pd
import psycopg2
import json
from psycopg2.extras import Json
from tqdm import tqdm
from src.utils.logger import migration_logger
from src.utils.validators import get_universal_comparator
from config.database import CONNECTION_STRING, engine

# Потрібно додати в кожен мігратор:
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class EkAddrMigrator:
    def __init__(self):
        self.connection = psycopg2.connect(CONNECTION_STRING)
        self.cursor = self.connection.cursor()
        self.logger = migration_logger
        self.stats = {
            'processed': 0, 
            'errors': 0, 
            'duplicates': 0, 
            'validated': 0,
            'similar_found': 0,
            'premises_created': 0
        }
        self.comparator = get_universal_comparator()
    
    def setup_source_tracking(self):
        """Налаштування відстеження джерела даних"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.data_sources (name, description) 
                VALUES (%s, %s) 
                ON CONFLICT (name) DO NOTHING
            """, ('ek_addr', 'Таблиця адрес ЕК (addr.ek_addr)'))
            self.connection.commit()
            self.logger.info("Джерело ek_addr зареєстровано")
        except Exception as e:
            self.logger.error(f"Помилка налаштування джерела: {e}")
    
    def get_source_id(self):
        """Отримання ID джерела"""
        try:
            self.cursor.execute("SELECT id FROM addrinity.data_sources WHERE name = 'ek_addr'")
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Помилка отримання ID джерела: {e}")
            return None
    
    def create_ek_addr_key(self, row):
        """Створення унікального ключа для ek_addr запису"""
        key_parts = [
            row['district'] or '',
            row['street_type'] or '',
            row['street'] or '',
            row['build'] or '',
            row['corp'] or ''
        ]
        return '|'.join(str(part) for part in key_parts if part)
    
    def get_or_create_city_for_ek(self, city_name='Дніпро'):
        """Отримання або створення міста для ek_addr"""
        try:
            # Пошук існуючого міста
            self.cursor.execute("""
                SELECT id FROM addrinity.cities 
                WHERE name_uk = %s
                LIMIT 1
            """, (city_name,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення ієрархії (спрощена версія)
            self.cursor.execute("""
                INSERT INTO addrinity.countries (iso_code, name_uk) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, ('UA', 'Україна'))
            
            self.cursor.execute("""
                SELECT id FROM addrinity.countries WHERE iso_code = 'UA'
            """)
            country_id = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                INSERT INTO addrinity.regions (country_id, name_uk) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (country_id, 'Дніпропетровська область'))
            
            self.cursor.execute("""
                SELECT id FROM addrinity.regions WHERE name_uk = 'Дніпропетровська область'
            """)
            region_id = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                INSERT INTO addrinity.cities (region_id, name_uk, type) 
                VALUES (%s, %s, %s) 
                RETURNING id
            """, (region_id, city_name, 'м.'))
            
            city_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return city_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення міста: {e}")
            raise
    
    def get_or_create_district_for_ek(self, city_id, district_name):
        """Отримання або створення району міста для ek_addr"""
        try:
            if not district_name:
                return None
            
            # Нормалізація назви району
            normalized_name = self.comparator.normalize_text(str(district_name), "district")
            
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.city_districts 
                WHERE city_id = %s AND (
                    name_uk = %s OR 
                    similarity(name_uk, %s) > 0.9
                )
            """, (city_id, normalized_name, normalized_name))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Валідація назви району
            validation_result = self.comparator.validate_object_universally(
                str(district_name), "district"
            )
            
            # Створення нового району
            self.cursor.execute("""
                INSERT INTO addrinity.city_districts 
                (city_id, name_uk, type) 
                VALUES (%s, %s, %s) 
                RETURNING id
            """, (city_id, normalized_name, 'адміністративний'))
            
            district_id = self.cursor.fetchone()[0]
            self.connection.commit()
            
            # Логування валідації
            if validation_result['similar_objects']:
                self.logger.info(f"Створено район '{normalized_name}' з можливими схожими: {len(validation_result['similar_objects'])}")
            
            return district_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення району міста: {e}")
            raise
    
    def get_or_create_street_type(self, type_name):
        """Отримання або створення типу вулиці"""
        try:
            if not type_name:
                type_name = 'вулиця'
            
            # Нормалізація типу вулиці
            normalized_type = self.comparator.normalize_text(str(type_name), "street_type")
            
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.street_types 
                WHERE ek_addr_type_code = %s OR name_uk = %s
            """, (str(type_name), normalized_type))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нового типу
            short_name = self.get_short_name_for_type(normalized_type)
            self.cursor.execute("""
                INSERT INTO addrinity.street_types 
                (name_uk, short_name_uk, ek_addr_type_code) 
                VALUES (%s, %s, %s) 
                RETURNING id
            """, (normalized_type, short_name, str(type_name)))
            
            type_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return type_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення типу вулиці: {e}")
            raise
    
    def get_short_name_for_type(self, full_type):
        """Отримання скороченої назви типу вулиці"""
        short_names = {
            'вулиця': 'вул.',
            'проспект': 'просп.',
            'бульвар': 'бул.',
            'провулок': 'пров.',
            'шосе': 'ш.',
            'тупик': 'туп.',
            'майдан': 'майд.',
            'житловий масив': 'ж/м',
            'алея': 'ал.'
        }
        return short_names.get(full_type.lower(), full_type[:4] + '.')
    
    def check_existing_street_entity(self, ek_addr_key):
        """Перевірка наявності вуличного об'єкта"""
        try:
            self.cursor.execute("""
                SELECT id FROM addrinity.street_entities 
                WHERE ek_addr_street_key = %s
            """, (ek_addr_key,))
            return self.cursor.fetchone()
        except Exception as e:
            return None
    
    def create_or_get_street_entity(self, row, city_id, district_id, street_type_id):
        """Створення або отримання вуличного об'єкта"""
        try:
            street_key = f"ek_{row['street']}_{row['street_type']}"
            
            # Перевірка наявності
            existing = self.check_existing_street_entity(street_key)
            if existing:
                return existing[0]
            
            # Валідація назви вулиці
            if row['street']:
                street_name = str(row['street'])
                validation_result = self.comparator.validate_object_universally(
                    street_name, "street"
                )
                
                # Створення нового вуличного об'єкта
                self.cursor.execute("""
                    INSERT INTO addrinity.street_entities 
                    (city_id, city_district_id, type_id, ek_addr_street_key) 
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (city_id, district_id, street_type_id, street_key))
                
                street_entity_id = self.cursor.fetchone()[0]
                
                # Додавання назви вулиці
                self.cursor.execute("""
                    INSERT INTO addrinity.street_names 
                    (street_entity_id, name, language_code, is_current, name_type)
                    VALUES (%s, %s, %s, %s, %s)
                """, (street_entity_id, street_name, 'uk', True, 'current'))
                
                self.connection.commit()
                
                # Логування валідації
                if validation_result['similar_objects']:
                    self.stats['similar_found'] += 1
                    self.logger.info(f"Створено вулицю '{street_name}' з можливими схожими: {len(validation_result['similar_objects'])}")
                
                return street_entity_id
            
            return None
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення вуличного об'єкта: {e}")
            raise
    
    def create_building_with_premises(self, row, street_entity_id, ek_addr_key):
        """Створення будівлі з квартирами/приміщеннями"""
        try:
            # Створення будівлі
            building_number = str(row['build']) if row['build'] else ''
            corpus = str(row['corp']) if row['corp'] else None
            
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.buildings 
                WHERE ek_addr_building_key = %s
            """, (ek_addr_key,))
            
            result = self.cursor.fetchone()
            if result:
                building_id = result[0]
            else:
                self.cursor.execute("""
                    INSERT INTO addrinity.buildings 
                    (street_entity_id, number, corpus, ek_addr_building_key)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (street_entity_id, building_number, corpus, ek_addr_key))
                
                building_id = self.cursor.fetchone()[0]
                self.connection.commit()
            
            # Створення приміщення (якщо є квартира)
            if row['flat']:
                premise_number = str(row['flat'])
                
                # Створення унікального ключа для приміщення
                premise_key = f"{ek_addr_key}_{premise_number}"
                
                # Перевірка наявності
                self.cursor.execute("""
                    SELECT id FROM addrinity.premises 
                    WHERE ek_addr_premise_key = %s
                """, (premise_key,))
                
                if not self.cursor.fetchone():
                    self.cursor.execute("""
                        INSERT INTO addrinity.premises 
                        (building_id, number, type, ek_addr_premise_key)
                        VALUES (%s, %s, %s, %s)
                    """, (building_id, premise_number, 'квартира', premise_key))
                    
                    self.stats['premises_created'] += 1
                    self.connection.commit()
            
            return building_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення будівлі/приміщення: {e}")
            raise
    
    def is_valid_record(self, row):
        """Валідація запису"""
        # Має бути хоча б вулиця або будівля
        if not row['street'] and not row['build']:
            return False, "Відсутня вулиця та будівля"
        
        return True, "OK"
    
    def process_single_row(self, row, source_id):
        """Обробка одного запису з повною валідацією"""
        try:
            # Базова валідація
            is_valid, message = self.is_valid_record(row)
            if not is_valid:
                self.stats['errors'] += 1
                self.logger.debug(f"Невалідний запис: {message}")
                return
            
            # Отримання міста
            city_id = self.get_or_create_city_for_ek()
            
            # Отримання району міста
            district_id = self.get_or_create_district_for_ek(city_id, row['district'])
            
            # Отримання типу вулиці
            street_type_id = self.get_or_create_street_type(row['street_type'])
            
            # Створення вуличного об'єкта (якщо є вулиця)
            street_entity_id = None
            if row['street']:
                street_entity_id = self.create_or_get_street_entity(
                    row, city_id, district_id, street_type_id
                )
            
            # Створення унікального ключа
            ek_addr_key = self.create_ek_addr_key(row)
            
            # Створення будівлі з приміщеннями
            building_id = None
            if row['build']:
                building_id = self.create_building_with_premises(row, street_entity_id, ek_addr_key)
            
            # Збереження зв'язку з джерелом
            original_data = {
                'district': str(row['district']) if row['district'] else None,
                'street_type': str(row['street_type']) if row['street_type'] else None,
                'street': str(row['street']) if row['street'] else None,
                'build': str(row['build']) if row['build'] else None,
                'corp': str(row['corp']) if row['corp'] else None,
                'flat': str(row['flat']) if row['flat'] else None
            }
            
            object_type = 'premise' if row['flat'] else 'building' if row['build'] else 'street'
            object_id = building_id or street_entity_id or city_id
            
            # Перевірка наявності джерела
            self.cursor.execute("""
                SELECT id FROM addrinity.object_sources 
                WHERE object_type = %s AND object_id = %s AND source_id = %s
            """, (object_type, object_id, source_id))
            
            if not self.cursor.fetchone():
                self.cursor.execute("""
                    INSERT INTO addrinity.object_sources 
                    (object_type, object_id, source_id, original_data)
                    VALUES (%s, %s, %s, %s)
                """, (object_type, object_id, source_id, json.dumps(original_data, ensure_ascii=False)))
            
            self.stats['processed'] += 1
            self.stats['validated'] += 1
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            self.stats['errors'] += 1
            self.logger.error(f"Помилка обробки запису: {e}")
    
    def migrate(self, dry_run=False, batch_size=1000):
        """Головний метод міграції"""
        if dry_run:
            self.logger.info("Тестовий запуск міграції ek_addr (без збереження)")
        
        try:
            # Налаштування джерела
            self.setup_source_tracking()
            source_id = self.get_source_id()
            
            # Отримання даних
            self.logger.info("Отримання даних з addr.ek_addr...")
            df = pd.read_sql("""
                SELECT * FROM addr.ek_addr 
                WHERE street IS NOT NULL OR build IS NOT NULL
            """, engine)
            
            total_records = len(df)
            self.logger.info(f"Знайдено {total_records} записів для міграції")
            
            if dry_run:
                df = df.head(100)
                total_records = len(df)
                self.logger.info(f"Тестовий запуск: обробляємо {total_records} записів")
            
            # Обробка по батчах
            processed = 0
            with tqdm(total=total_records, desc="Міграція ek_addr") as pbar:
                for _, row in df.iterrows():
                    if not dry_run:
                        self.process_single_row(row, source_id)
                    else:
                        is_valid, _ = self.is_valid_record(row)
                        if is_valid:
                            self.stats['processed'] += 1
                        else:
                            self.stats['errors'] += 1
                    
                    processed += 1
                    pbar.update(1)
                    
                    if not dry_run and processed % 1000 == 0:
                        self.logger.info(f"Оброблено {processed} записів")
            
            # Вивід статистики
            self.logger.info(f"""
            Статистика міграції ek_addr:
            - Оброблено: {self.stats['processed']}
            - Помилок: {self.stats['errors']}
            - Дублікатів: {self.stats['duplicates']}
            - Валідовано: {self.stats['validated']}
            - Схожих знайдено: {self.stats['similar_found']}
            - Приміщень створено: {self.stats['premises_created']}
            """)
            
            if dry_run:
                self.logger.info("Тестовий запуск завершено (дані не збережено)")
            
        except Exception as e:
            self.logger.error(f"Критична помилка міграції: {e}")
            raise
        finally:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
                