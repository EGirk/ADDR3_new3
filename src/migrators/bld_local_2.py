"""Мігратор для таблиці addr.bld_local"""

import pandas as pd
import psycopg2
import json
from tqdm import tqdm
from src.utils.logger import migration_logger
from config.database import CONNECTION_STRING

class BldLocalMigrator:
    def __init__(self):
        self.connection = psycopg2.connect(CONNECTION_STRING)
        self.cursor = self.connection.cursor()
        self.logger = migration_logger
        self.stats = {'processed': 0, 'errors': 0, 'duplicates': 0}
    
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
        """Створення ієрархії України"""
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
        """Отримання або створення району міста"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.city_districts 
                (city_id, name_uk, type, bld_local_raion_name) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (city_id, name_uk) DO NOTHING
            """, (city_id, district_name, 'адміністративний', district_name))
            
            # Отримуємо ID створеного або існуючого запису
            self.cursor.execute("""
                SELECT id FROM addrinity.city_districts 
                WHERE city_id = %s AND name_uk = %s
            """, (city_id, district_name))
            
            result = self.cursor.fetchone()
            district_id = result[0] if result else None
            
            self.connection.commit()
            return district_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення району міста: {e}")
            raise
    
    def get_or_create_street_type(self, type_name, short_name=None):
        """Отримання або створення типу вулиці"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.street_types 
                (name_uk, short_name_uk, bld_local_type_code) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (name_uk) DO NOTHING
            """, (type_name, short_name, type_name))
            
            # Отримуємо ID створеного або існуючого запису
            self.cursor.execute("""
                SELECT id FROM addrinity.street_types 
                WHERE name_uk = %s
            """, (type_name,))
            
            result = self.cursor.fetchone()
            type_id = result[0] if result else None
            
            self.connection.commit()
            return type_id
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Помилка створення типу вулиці: {e}")
            raise
    
    def is_valid_record(self, row):
        """Перевірка чи запис валідний для міграції"""
        # Базові обов'язкові поля
        if not row['objectid']:
            return False
        
        # Має бути хоча б одна непуста адреса
        if not row['adres_n_uk'] and not row['adres_o_uk']:
            return False
        
        # Має бути назва вулиці
        if not row['street_ukr'] or not str(row['street_ukr']).strip():
            return False
        
        # Має бути район
        if not row['raion'] or not str(row['raion']).strip():
            return False
        
        return True
    
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
    
    def process_single_row(self, row, source_id, city_id):
        """Обробка одного рядка з bld_local"""
        try:
            # Валідація запису
            if not self.is_valid_record(row):
                self.stats['errors'] += 1
                return
            
            # Отримання або створення району міста
            raion_name = str(row['raion']) if row['raion'] and str(row['raion']).strip() else 'Невідомий'
            city_district_id = self.get_or_create_city_district(raion_name, city_id)
            
            # Отримання або створення типу вулиці
            type_name = str(row['type_ukr']) if row['type_ukr'] else 'ВУЛ.'
            street_type_id = self.get_or_create_street_type(type_name, 'вул.')
            
            # Перевірка наявності вуличного об'єкта
            self.cursor.execute("""
                SELECT id FROM addrinity.street_entities 
                WHERE bld_local_objectid = %s
            """, (row['objectid'],))
            
            if self.cursor.fetchone():
                self.stats['duplicates'] += 1
                return  # Вже існує
            
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
            if row['street_ukr']:
                # Поточна назва
                self.cursor.execute("""
                    INSERT INTO addrinity.street_names 
                    (street_entity_id, name, language_code, is_current, name_type)
                    VALUES (%s, %s, %s, %s, %s)
                """, (street_entity_id, str(row['street_ukr']), 'uk', True, 'current'))
                
                # Стара назва (якщо відрізняється)
                old_street = self.extract_street_from_address(str(row['adres_o_uk']))
                if old_street and old_street != str(row['street_ukr']):
                    self.cursor.execute("""
                        INSERT INTO addrinity.street_names 
                        (street_entity_id, name, language_code, is_current, name_type)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (street_entity_id, old_street, 'uk', False, 'old'))
            
            # Створення будівлі
            building_number = str(row['l']) if row['l'] else ''
            building_key = f"bld_local_{row['objectid']}"  # Унікальний ключ
            
            self.cursor.execute("""
                INSERT INTO addrinity.buildings 
                (street_entity_id, number, building_key,
                 bld_local_objectid, bld_local_id_bld_rtg)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (building_key) DO UPDATE SET
                    street_entity_id = EXCLUDED.street_entity_id
                RETURNING id
            """, (street_entity_id, building_number, building_key,
                  row['objectid'], row['id_bld_rtg']))
            
            building_id = self.cursor.fetchone()[0]
            
            # Збереження зв'язку з джерелом
            original_data = {
                'objectid': int(row['objectid']) if row['objectid'] else None,
                'adres_n_uk': str(row['adres_n_uk']) if row['adres_n_uk'] else None,
                'adres_o_uk': str(row['adres_o_uk']) if row['adres_o_uk'] else None,
                'raion': str(row['raion']) if row['raion'] else None,
                'street_ukr': str(row['street_ukr']) if row['street_ukr'] else None,
                'building_number': building_number
            }
            
            self.cursor.execute("""
                INSERT INTO addrinity.object_sources 
                (object_type, object_id, source_id, original_data)
                VALUES (%s, %s, %s, %s)
            """, ('building', building_id, source_id, json.dumps(original_data, ensure_ascii=False)))
            
            self.stats['processed'] += 1
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
            """, self.connection)
            
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
                        if self.is_valid_record(row):
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