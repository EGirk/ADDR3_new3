"""Повний мігратор для addr.rtg_addr з універсальним валідатором"""

import pandas as pd
import psycopg2
import json
from tqdm import tqdm
from src.utils.logger import migration_logger
from src.utils.validators import get_universal_comparator
from config.database import CONNECTION_STRING

class RtgAddrMigrator:
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
            """, ('rtg_addr', 'Таблиця адрес RTG (addr.rtg_addr)'))
            self.connection.commit()
            self.logger.info("Джерело rtg_addr зареєстровано")
        except Exception as e:
            self.logger.error(f"Помилка налаштування джерела: {e}")
    
    def get_source_id(self):
        """Отримання ID джерела"""
        try:
            self.cursor.execute("SELECT id FROM addrinity.data_sources WHERE name = 'rtg_addr'")
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Помилка отримання ID джерела: {e}")
            return None
    
    def parse_path_hierarchy(self, path_str):
        """Парсинг ltree path в ієрархію"""
        if not path_str:
            return {}
        
        ids = str(path_str).split('.')
        
        levels = {
            'country': ids[0] if len(ids) > 0 else None,
            'region': ids[1] if len(ids) > 1 else None,
            'district': ids[2] if len(ids) > 2 else None,
            'community': ids[3] if len(ids) > 3 else None,
            'city': ids[4] if len(ids) > 4 else None,
            'city_district': ids[5] if len(ids) > 5 else None,
            'object': ids[6] if len(ids) > 6 else None
        }
        
        return levels
    
    def get_or_create_country(self, country_id_from_path, country_name=None):
        """Отримання або створення країни"""
        try:
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.countries 
                WHERE rtg_country_id = %s
            """, (country_id_from_path,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нової країни
            country_name = country_name or 'Україна'
            self.cursor.execute("""
                INSERT INTO addrinity.countries 
                (iso_code, name_uk, rtg_country_id) 
                VALUES (%s, %s, %s) 
                RETURNING id
            """, ('UA', country_name, country_id_from_path))
            
            country_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return country_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення країни: {e}")
            raise
    
    def get_or_create_region(self, region_id_from_path, country_id, region_name=None):
        """Отримання або створення регіону"""
        try:
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.regions 
                WHERE rtg_region_id = %s
            """, (region_id_from_path,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нового регіону
            region_name = region_name or 'Дніпропетровська область'
            self.cursor.execute("""
                INSERT INTO addrinity.regions 
                (country_id, name_uk, rtg_region_id) 
                VALUES (%s, %s, %s) 
                RETURNING id
            """, (country_id, region_name, region_id_from_path))
            
            region_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return region_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення регіону: {e}")
            raise
    
    def get_or_create_district(self, district_id_from_path, region_id, district_name=None):
        """Отримання або створення району"""
        try:
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.districts 
                WHERE rtg_district_id = %s
            """, (district_id_from_path,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нового району
            district_name = district_name or 'Дніпровський район'
            self.cursor.execute("""
                INSERT INTO addrinity.districts 
                (region_id, name_uk, rtg_district_id) 
                VALUES (%s, %s, %s) 
                RETURNING id
            """, (region_id, district_name, district_id_from_path))
            
            district_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return district_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення району: {e}")
            raise
    
    def get_or_create_community(self, community_id_from_path, district_id, community_name=None):
        """Отримання або створення громади"""
        try:
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.communities 
                WHERE rtg_community_id = %s
            """, (community_id_from_path,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нової громади
            community_name = community_name or 'Дніпровська міська громада'
            self.cursor.execute("""
                INSERT INTO addrinity.communities 
                (district_id, name_uk, type, rtg_community_id) 
                VALUES (%s, %s, %s, %s) 
                RETURNING id
            """, (district_id, community_name, 'міська', community_id_from_path))
            
            community_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return community_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення громади: {e}")
            raise
    
    def get_or_create_city(self, city_id_from_path, community_id, city_name=None):
        """Отримання або створення міста"""
        try:
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.cities 
                WHERE rtg_city_id = %s
            """, (city_id_from_path,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нового міста
            city_name = city_name or 'Дніпро'
            self.cursor.execute("""
                INSERT INTO addrinity.cities 
                (community_id, name_uk, type, rtg_city_id) 
                VALUES (%s, %s, %s, %s) 
                RETURNING id
            """, (community_id, city_name, 'м.', city_id_from_path))
            
            city_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return city_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення міста: {e}")
            raise
    
    def get_or_create_city_district(self, city_district_id_from_path, city_id, district_name):
        """Отримання або створення району міста"""
        try:
            if not district_name:
                return None
            
            # Нормалізація назви району
            normalized_name = self.comparator.normalize_text(str(district_name), "district")
            
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.city_districts 
                WHERE rtg_city_district_id = %s
            """, (city_district_id_from_path,))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Валідація назви району
            validation_result = self.comparator.validate_object_universally(
                str(district_name), "district"
            )
            
            # Створення нового району міста
            self.cursor.execute("""
                INSERT INTO addrinity.city_districts 
                (city_id, name_uk, type, rtg_city_district_id) 
                VALUES (%s, %s, %s, %s) 
                RETURNING id
            """, (city_id, normalized_name, 'адміністративний', city_district_id_from_path))
            
            city_district_id = self.cursor.fetchone()[0]
            self.connection.commit()
            
            # Логування валідації
            if validation_result['similar_objects']:
                self.logger.info(f"Створено район міста '{normalized_name}' з можливими схожими: {len(validation_result['similar_objects'])}")
            
            return city_district_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення району міста: {e}")
            raise
    
    def get_or_create_street_type(self, type_name):
        """Отримання або створення типу вулиці"""
        try:
            if not type_name:
                type_name = "вулиця"
            
            # Нормалізація типу вулиці
            normalized_type = self.comparator.normalize_text(str(type_name), "street_type")
            
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.street_types 
                WHERE rtg_type_code = %s OR name_uk = %s
            """, (str(type_name), normalized_type))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Створення нового типу
            short_name = self.get_short_name_for_type(normalized_type)
            self.cursor.execute("""
                INSERT INTO addrinity.street_types 
                (name_uk, short_name_uk, rtg_type_code) 
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
    
    def check_existing_street_entity(self, path):
        """Перевірка наявності вуличного об'єкта"""
        try:
            self.cursor.execute("""
                SELECT id FROM addrinity.street_entities 
                WHERE rtg_path = %s
            """, (str(path),))
            return self.cursor.fetchone()
        except Exception as e:
            return None
    
    def create_building_with_premises(self, row, street_entity_id):
        """Створення будівлі з квартирами/приміщеннями"""
        try:
            # Створення будівлі
            building_number = str(row['building']) if row['building'] else ''
            
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.buildings 
                WHERE rtg_building_id = %s
            """, (row['id'],))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            self.cursor.execute("""
                INSERT INTO addrinity.buildings 
                (street_entity_id, number, rtg_building_id)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (street_entity_id, building_number, row['id']))
            
            building_id = self.cursor.fetchone()[0]
            
            # Створення приміщення (якщо є)
            if row['flat'] or row['room']:
                premise_number = str(row['flat'] or row['room'])
                premise_type = 'квартира' if row['flat'] else 'кімната'
                floor = str(row['floor']) if row['floor'] else None
                entrance = str(row['entrance']) if row['entrance'] else None
                
                # Перевірка наявності приміщення
                self.cursor.execute("""
                    SELECT id FROM addrinity.premises 
                    WHERE rtg_premise_id = %s
                """, (row['id'],))
                
                if not self.cursor.fetchone():
                    self.cursor.execute("""
                        INSERT INTO addrinity.premises 
                        (building_id, number, floor, entrance, type, rtg_premise_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (building_id, premise_number, floor, entrance, premise_type, row['id']))
                    
                    self.stats['premises_created'] += 1
            
            self.connection.commit()
            return building_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення будівлі/приміщення: {e}")
            raise
    
    def process_single_row(self, row, source_id):
        """Обробка одного запису з повною валідацією"""
        try:
            # Парсинг path
            path_levels = self.parse_path_hierarchy(row['path'])
            
            if not path_levels.get('country') or not path_levels.get('city'):
                self.stats['errors'] += 1
                return
            
            # Створення ієрархії
            country_id = self.get_or_create_country(path_levels['country'], row['region'])
            region_id = self.get_or_create_region(path_levels['region'], country_id, row['region'])
            district_id = self.get_or_create_district(path_levels['district'], region_id, row['district'])
            community_id = self.get_or_create_community(path_levels['community'], district_id, row['community'])
            city_id = self.get_or_create_city(path_levels['city'], community_id, row['city'])
            
            # Район міста (якщо є)
            city_district_id = None
            if path_levels.get('city_district') and row['city_district']:
                city_district_id = self.get_or_create_city_district(
                    path_levels['city_district'], city_id, row['city_district']
                )
            
            # Тип вулиці
            street_type_id = self.get_or_create_street_type(row['street_type'])
            
            # Вуличний об'єкт
            if row['street']:
                # Перевірка наявності
                existing_street = self.check_existing_street_entity(row['path'])
                if existing_street:
                    street_entity_id = existing_street[0]
                    self.stats['duplicates'] += 1
                else:
                    # Валідація назви вулиці
                    street_name = str(row['street'])
                    validation_result = self.comparator.validate_object_universally(
                        street_name, "street"
                    )
                    
                    # Створення вуличного об'єкта
                    self.cursor.execute("""
                        INSERT INTO addrinity.street_entities 
                        (city_id, city_district_id, type_id, rtg_path, rtg_street_id) 
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                    """, (city_id, city_district_id, street_type_id, str(row['path']), row['id']))
                    
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
            else:
                street_entity_id = None
            
            # Будівля з приміщеннями
            if row['building'] or row['flat'] or row['room']:
                building_id = self.create_building_with_premises(row, street_entity_id)
            else:
                building_id = None
            
            # Збереження зв'язку з джерелом
            original_data = {
                'id': int(row['id']) if row['id'] else None,
                'path': str(row['path']) if row['path'] else None,
                'city': str(row['city']) if row['city'] else None,
                'street': str(row['street']) if row['street'] else None,
                'building': str(row['building']) if row['building'] else None,
                'flat': str(row['flat']) if row['flat'] else None,
                'room': str(row['room']) if row['room'] else None
            }
            
            object_type = 'premise' if row['flat'] or row['room'] else 'building' if row['building'] else 'street'
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
            
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"Помилка обробки запису {row.get('id', 'unknown')}: {e}")
    
    def migrate(self, dry_run=False, batch_size=1000):
        """Головний метод міграції"""
        if dry_run:
            self.logger.info("Тестовий запуск міграції rtg_addr (без збереження)")
        
        try:
            # Налаштування джерела
            self.setup_source_tracking()
            source_id = self.get_source_id()
            
            # Отримання даних
            self.logger.info("Отримання даних з addr.rtg_addr...")
            df = pd.read_sql("""
                SELECT * FROM addr.rtg_addr 
                WHERE path IS NOT NULL
            """, self.connection)
            
            total_records = len(df)
            self.logger.info(f"Знайдено {total_records} записів для міграції")
            
            if dry_run:
                df = df.head(100)
                total_records = len(df)
                self.logger.info(f"Тестовий запуск: обробляємо {total_records} записів")
            
            # Обробка по батчах
            processed = 0
            with tqdm(total=total_records, desc="Міграція rtg_addr") as pbar:
                for _, row in df.iterrows():
                    if not dry_run:
                        self.process_single_row(row, source_id)
                    else:
                        self.stats['processed'] += 1
                    
                    processed += 1
                    pbar.update(1)
                    
                    if not dry_run and processed % 1000 == 0:
                        self.logger.info(f"Оброблено {processed} записів")
            
            # Вивід статистики
            self.logger.info(f"""
            Статистика міграції rtg_addr:
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


                