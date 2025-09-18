"""Повний мігратор для addr.bld_local з універсальним валідатором"""

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


class BldLocalMigrator:
    def __init__(self):
        self.connection = psycopg2.connect(CONNECTION_STRING)
        self.cursor = self.connection.cursor()
        self.logger = migration_logger
        self.stats = {
            'processed': 0, 
            'errors': 0, 
            'duplicates': 0, 
            'validated': 0,
            'similar_found': 0
        }
        self.comparator = get_universal_comparator()
    
    def setup_source_tracking(self):
        """Налаштування відстеження джерела даних"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.data_sources (name, description) 
                VALUES (%s, %s) 
                ON CONFLICT (name) DO NOTHING
            """, ('bld_local', 'Локальна таблиця будівель (bld_local)'))
            self.connection.commit()
            self.logger.info("Джерело bld_local зареєстровано")
        except Exception as e:
            self.logger.error(f"Помилка налаштування джерела: {e}")
    
    def get_source_id(self):
        """Отримання ID джерела"""
        try:
            self.cursor.execute("SELECT id FROM addrinity.data_sources WHERE name = 'bld_local'")
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Помилка отримання ID джерела: {e}")
            return None
    
    def create_ukraine_hierarchy(self):
        """Створення базової ієрархії України"""
        try:
            # Країна
            self.cursor.execute("""
                INSERT INTO addrinity.countries (iso_code, name_uk, bld_local_country_code) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (iso_code) DO NOTHING
                RETURNING id
            """, ('UA', 'Україна', 'UA'))
            
            if self.cursor.rowcount == 0:
                self.cursor.execute("SELECT id FROM addrinity.countries WHERE iso_code = 'UA'")
            country_id = self.cursor.fetchone()[0]
            
            # Регіон
            self.cursor.execute("""
                INSERT INTO addrinity.regions (country_id, name_uk, bld_local_region_key) 
                VALUES (%s, %s, %s) 
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (country_id, 'Дніпропетровська область', 'dnipropetrovsk'))
            
            if self.cursor.rowcount == 0:
                self.cursor.execute("""
                    SELECT id FROM addrinity.regions 
                    WHERE name_uk = 'Дніпропетровська область'
                """)
            region_id = self.cursor.fetchone()[0]
            
            # Район
            self.cursor.execute("""
                INSERT INTO addrinity.districts (region_id, name_uk, bld_local_district_key) 
                VALUES (%s, %s, %s) 
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (region_id, 'Дніпровський район', 'dnipro_district'))
            
            if self.cursor.rowcount == 0:
                self.cursor.execute("""
                    SELECT id FROM addrinity.districts 
                    WHERE name_uk = 'Дніпровський район'
                """)
            district_id = self.cursor.fetchone()[0]
            
            # Громада
            self.cursor.execute("""
                INSERT INTO addrinity.communities (district_id, name_uk, type, bld_local_community_key) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (district_id, 'Дніпровська міська громада', 'міська', 'dnipro_community'))
            
            if self.cursor.rowcount == 0:
                self.cursor.execute("""
                    SELECT id FROM addrinity.communities 
                    WHERE name_uk = 'Дніпровська міська громада'
                """)
            community_id = self.cursor.fetchone()[0]
            
            # Місто
            self.cursor.execute("""
                INSERT INTO addrinity.cities (community_id, name_uk, type, bld_local_city_key) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (community_id, 'Дніпро', 'м.', 'dnipro_city'))
            
            if self.cursor.rowcount == 0:
                self.cursor.execute("""
                    SELECT id FROM addrinity.cities WHERE name_uk = 'Дніпро'
                """)
            city_id = self.cursor.fetchone()[0]
            
            self.connection.commit()
            return country_id, region_id, district_id, community_id, city_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення ієрархії: {e}")
            raise
    
    def get_or_create_city_district(self, district_name, city_id):
        """Отримання або створення району міста з валідацією"""
        try:
            # Нормалізація назви району
            normalized_name = self.comparator.normalize_text(str(district_name), "district")
            
            # Перевірка наявності схожих районів
            validation_result = self.comparator.validate_object_universally(
                str(district_name), "district"
            )
            
            # Пошук існуючого району
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
            
            # Створення нового району
            self.cursor.execute("""
                INSERT INTO addrinity.city_districts 
                (city_id, name_uk, type, bld_local_raion_name) 
                VALUES (%s, %s, %s, %s) 
                RETURNING id
            """, (city_id, normalized_name, 'адміністративний', str(district_name)))
            
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
        """Отримання або створення типу вулиці з валідацією"""
        try:
            if not type_name:
                type_name = "ВУЛ."
            
            # Нормалізація типу вулиці
            normalized_type = self.comparator.normalize_text(str(type_name), "street_type")
            
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.street_types 
                WHERE name_uk = %s OR short_name_uk = %s
            """, (normalized_type, str(type_name)))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нового типу
            short_name = self.get_short_name_for_type(normalized_type)
            self.cursor.execute("""
                INSERT INTO addrinity.street_types 
                (name_uk, short_name_uk, bld_local_type_code) 
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
            'алея': 'ал.'
        }
        return short_names.get(full_type.lower(), full_type[:4] + '.')
    
    def is_valid_record(self, row):
        """Валідація запису"""
        if not row['objectid']:
            return False, "Відсутній objectid"
        
        if not row['adres_n_uk'] and not row['adres_o_uk']:
            return False, "Відсутні адреси"
        
        if not row['street_ukr'] or not str(row['street_ukr']).strip():
            return False, "Відсутня назва вулиці"
        
        if not row['raion'] or not str(row['raion']).strip():
            return False, "Відсутній район"
        
        return True, "OK"
    
    def extract_street_from_address(self, address):
        """Витяг назви вулиці з адресного рядка"""
        if not address or not isinstance(address, str):
            return None
        
        parts = address.strip().split()
        if len(parts) > 2:
            # Видаляємо перший (номер будинку) і останній (тип вулиці) елементи
            return ' '.join(parts[1:-1])
        elif len(parts) == 2:
            return parts[1]
        return address
    
    def check_existing_street_entity(self, objectid):
        """Перевірка наявності вуличного об'єкта"""
        try:
            self.cursor.execute("""
                SELECT id FROM addrinity.street_entities 
                WHERE bld_local_objectid = %s
            """, (objectid,))
            return self.cursor.fetchone()
        except Exception as e:
            return None
    
    def process_single_row(self, row, source_id, city_id):
        """Обробка одного запису з повною валідацією"""
        try:
            # Базова валідація
            is_valid, message = self.is_valid_record(row)
            if not is_valid:
                self.stats['errors'] += 1
                self.logger.debug(f"Невалідний запис {row.get('objectid', 'unknown')}: {message}")
                return
            
            # Перевірка дублікатів
            if self.check_existing_street_entity(row['objectid']):
                self.stats['duplicates'] += 1
                return
            
            # Валідація назви вулиці
            street_name = str(row['street_ukr']).strip()
            validation_result = self.comparator.validate_object_universally(
                street_name, "street"
            )
            
            # Отримання району міста
            raion_name = str(row['raion']) if row['raion'] and str(row['raion']).strip() else 'Невідомий'
            city_district_id = self.get_or_create_city_district(raion_name, city_id)
            
            # Отримання типу вулиці
            type_name = str(row['type_ukr']) if row['type_ukr'] else 'ВУЛ.'
            street_type_id = self.get_or_create_street_type(type_name)
            
            # Створення вуличного об'єкта
            self.cursor.execute("""
                INSERT INTO addrinity.street_entities 
                (city_id, city_district_id, type_id, 
                 bld_local_objectid, bld_local_id_street_rtg) 
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (city_id, city_district_id, street_type_id,
                  row['objectid'], row['id_street_rtg']))
            
            street_entity_id = self.cursor.fetchone()[0]
            
            # Додавання назв вулиці
            # Поточна назва
            self.cursor.execute("""
                INSERT INTO addrinity.street_names 
                (street_entity_id, name, language_code, is_current, name_type)
                VALUES (%s, %s, %s, %s, %s)
            """, (street_entity_id, street_name, 'uk', True, 'current'))
            
            # Стара назва (якщо відрізняється)
            old_street = self.extract_street_from_address(str(row['adres_o_uk']))
            if old_street and old_street != street_name:
                # Валідація старої назви
                old_validation = self.comparator.validate_object_universally(
                    old_street, "street"
                )
                
                self.cursor.execute("""
                    INSERT INTO addrinity.street_names 
                    (street_entity_id, name, language_code, is_current, name_type)
                    VALUES (%s, %s, %s, %s, %s)
                """, (street_entity_id, old_street, 'uk', False, 'old'))
                
                if old_validation['similar_objects']:
                    self.stats['similar_found'] += 1
            
            # Створення будівлі
            building_number = str(row['l']) if row['l'] else ''
            
            self.cursor.execute("""
                INSERT INTO addrinity.buildings 
                (street_entity_id, number,
                 bld_local_objectid, bld_local_id_bld_rtg)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (street_entity_id, building_number,
                  row['objectid'], row['id_bld_rtg']))
            
            building_id = self.cursor.fetchone()[0]
            
            # Збереження джерела
            original_data = {
                'objectid': int(row['objectid']) if row['objectid'] else None,
                'adres_n_uk': str(row['adres_n_uk']) if row['adres_n_uk'] else None,
                'adres_o_uk': str(row['adres_o_uk']) if row['adres_o_uk'] else None,
                'raion': raion_name,
                'street_ukr': street_name,
                'building_number': building_number,
                'validation_result': {
                    'confidence': validation_result['confidence_level'],
                    'similar_objects_count': len(validation_result['similar_objects'])
                }
            }
            
            self.cursor.execute("""
                INSERT INTO addrinity.object_sources 
                (object_type, object_id, source_id, original_data)
                VALUES (%s, %s, %s, %s)
            """, ('building', building_id, source_id, 
                  json.dumps(original_data, ensure_ascii=False)))
            
            self.stats['processed'] += 1
            self.stats['validated'] += 1
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            self.stats['errors'] += 1
            self.logger.error(f"Помилка обробки запису {row.get('objectid', 'unknown')}: {e}")
    
    def migrate(self, dry_run=False, batch_size=1000):
        """Головний метод міграції"""
        if dry_run:
            self.logger.info("Тестовий запуск міграції bld_local (без збереження)")
        
        try:
            # Налаштування джерела
            self.setup_source_tracking()
            source_id = self.get_source_id()
            
            # Створення базової ієрархії
            country_id, region_id, district_id, community_id, city_id = self.create_ukraine_hierarchy()
            
            # Отримання даних
            self.logger.info("Отримання даних з addr.bld_local...")
            df = pd.read_sql("""
                SELECT * FROM addr.bld_local 
                WHERE adres_n_uk IS NOT NULL AND street_ukr IS NOT NULL
            """, engine)
            
            total_records = len(df)
            self.logger.info(f"Знайдено {total_records} записів для міграції")
            
            if dry_run:
                # Обмежуємо кількість записів для тестового запуску
                df = df.head(100)
                total_records = len(df)
                self.logger.info(f"Тестовий запуск: обробляємо {total_records} записів")
            
            # Обробка по батчах
            processed = 0
            with tqdm(total=total_records, desc="Міграція bld_local") as pbar:
                for _, row in df.iterrows():
                    if not dry_run:
                        self.process_single_row(row, source_id, city_id)
                    else:
                        # Для тестового запуску просто симулюємо
                        is_valid, _ = self.is_valid_record(row)
                        if is_valid:
                            self.stats['processed'] += 1
                        else:
                            self.stats['errors'] += 1
                    
                    processed += 1
                    pbar.update(1)
                    
                    # Коміт кожні 1000 записів
                    if not dry_run and processed % 1000 == 0:
                        self.logger.info(f"Оброблено {processed} записів")
            
            # Вивід статистики
            self.logger.info(f"""
            Статистика міграції bld_local:
            - Оброблено: {self.stats['processed']}
            - Помилок: {self.stats['errors']}
            - Дублікатів: {self.stats['duplicates']}
            - Валідовано: {self.stats['validated']}
            - Схожих знайдено: {self.stats['similar_found']}
            - Всього: {self.stats['processed'] + self.stats['errors'] + self.stats['duplicates']}
            """)
            
            if dry_run:
                self.logger.info("Тестовий запуск завершено (дані не збережено)")
            
        except Exception as e:
            self.logger.error(f"Критична помилка міграції: {e}")
            raise
        finally:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()

# Аналогічно для інших міграторів...

