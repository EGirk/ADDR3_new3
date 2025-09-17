"""Мігратор для таблиці addr.rtg_addr"""

import pandas as pd
import psycopg2
import json
from tqdm import tqdm
from src.utils.logger import migration_logger
from config.database import CONNECTION_STRING

class RtgAddrMigrator:
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
            """, ('rtg_addr', 'Таблиця адрес RTG (addr.rtg_addr)'))
            self.connection.commit()
            self.logger.info("Джерело rtg_addr зареєстровано")
        except Exception as e:
            self.logger.error(f"Помилка налаштування джерела: {e}")

    def get_source_id(self):
        """Отримання ID джерела"""
        self.cursor.execute("SELECT id FROM addrinity.data_sources WHERE name = 'rtg_addr'")
        return self.cursor.fetchone()[0]

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

    def create_or_get_country(self, country_id_from_path):
        """Створення або отримання країни"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.countries
                (iso_code, name_uk, rtg_country_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (rtg_country_id) DO UPDATE SET
                    rtg_country_id = EXCLUDED.rtg_country_id
                RETURNING id
            """, ('UA', 'Україна', country_id_from_path))

            country_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return country_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_or_get_region(self, region_id_from_path, country_id, region_name=None):
        """Створення або отримання регіону"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.regions
                (country_id, name_uk, rtg_region_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (rtg_region_id) DO UPDATE SET
                    country_id = EXCLUDED.country_id
                RETURNING id
            """, (country_id, region_name or 'Дніпропетровська область', region_id_from_path))

            region_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return region_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_or_get_district(self, district_id_from_path, region_id, district_name=None):
        """Створення або отримання району"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.districts
                (region_id, name_uk, rtg_district_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (rtg_district_id) DO UPDATE SET
                    region_id = EXCLUDED.region_id
                RETURNING id
            """, (region_id, district_name or 'Дніпровський район', district_id_from_path))

            district_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return district_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_or_get_community(self, community_id_from_path, district_id, community_name=None):
        """Створення або отримання громади"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.communities
                (district_id, name_uk, type, rtg_community_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rtg_community_id) DO UPDATE SET
                    district_id = EXCLUDED.district_id
                RETURNING id
            """, (district_id, community_name or 'Дніпровська міська громада', 'міська', community_id_from_path))

            community_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return community_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_or_get_city(self, city_id_from_path, community_id, city_name=None):
        """Створення або отримання міста"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.cities
                (community_id, name_uk, type, rtg_city_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rtg_city_id) DO UPDATE SET
                    community_id = EXCLUDED.community_id
                RETURNING id
            """, (community_id, city_name or 'Дніпро', 'м.', city_id_from_path))

            city_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return city_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_or_get_city_district(self, city_district_id_from_path, city_id, district_name):
        """Створення або отримання району міста"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.city_districts
                (city_id, name_uk, type, rtg_city_district_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rtg_city_district_id) DO UPDATE SET
                    city_id = EXCLUDED.city_id
                RETURNING id
            """, (city_id, district_name, 'адміністративний', city_district_id_from_path))

            city_district_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return city_district_id
        except Exception as e:
            self.connection.rollback()
            raise

    def get_or_create_street_type(self, type_name, short_name=None):
        """Отримання або створення типу вулиці"""
        try:
            self.cursor.execute("""
                INSERT INTO addrinity.street_types
                (name_uk, short_name_uk, rtg_type_code)
                VALUES (%s, %s, %s)
                ON CONFLICT (rtg_type_code) DO UPDATE SET
                    name_uk = EXCLUDED.name_uk
                RETURNING id
            """, (type_name or 'вулиця', short_name or 'вул.', type_name))

            type_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return type_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_or_get_street_entity(self, row, city_id, city_district_id, street_type_id):
        """Створення або отримання вуличного об'єкта"""
        try:
            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.street_entities
                WHERE rtg_path = %s
            """, (str(row['path']),))

            result = self.cursor.fetchone()
            if result:
                return result[0]

            # Створення нового
            self.cursor.execute("""
                INSERT INTO addrinity.street_entities
                (city_id, city_district_id, type_id, rtg_path, rtg_street_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (city_id, city_district_id, street_type_id, str(row['path']), row['id']))

            street_entity_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return street_entity_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_building_with_premises(self, row, street_entity_id):
        """Створення будівлі з квартирами/приміщеннями"""
        try:
            # Створення унікального ключа для будівлі
            building_key = f"{row['path']}_{row['building'] or 'no_building'}"

            # Створення будівлі
            self.cursor.execute("""
                INSERT INTO addrinity.buildings
                (street_entity_id, number, postal_code, building_key, rtg_building_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (building_key) DO UPDATE SET
                    street_entity_id = EXCLUDED.street_entity_id
                RETURNING id
            """, (street_entity_id, row['building'] or '', row['postal_code'], building_key, row['id']))

            building_id = self.cursor.fetchone()[0]

            # Створення приміщення (якщо є)
            if row['flat'] or row['room']:
                premise_number = row['flat'] or row['room']
                premise_type = 'квартира' if row['flat'] else 'кімната'

                self.cursor.execute("""
                    INSERT INTO addrinity.premises
                    (building_id, number, floor, entrance, type, rtg_premise_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (building_id, premise_number, row['floor'], row['entrance'], premise_type, row['id']))

            self.connection.commit()
            return building_id
        except Exception as e:
            self.connection.rollback()
            raise

    def process_single_row(self, row, source_id):
        """Обробка одного рядка з rtg_addr"""
        try:
            # Парсинг path
            path_levels = self.parse_path_hierarchy(row['path'])

            if not path_levels.get('country') or not path_levels.get('city'):
                self.stats['errors'] += 1
                return

            # Створення ієрархії
            country_id = self.create_or_get_country(path_levels['country'])
            region_id = self.create_or_get_region(path_levels['region'], country_id, row['region'])
            district_id = self.create_or_get_district(path_levels['district'], region_id, row['district'])
            community_id = self.create_or_get_community(path_levels['community'], district_id, row['community'])
            city_id = self.create_or_get_city(path_levels['city'], community_id, row['city'])

            # Район міста (якщо є)
            city_district_id = None
            if path_levels.get('city_district') and row['city_district']:
                city_district_id = self.create_or_get_city_district(
                    path_levels['city_district'], city_id, row['city_district']
                )

            # Тип вулиці
            street_type_id = self.get_or_create_street_type(row['street_type'])

            # Вуличний об'єкт
            if row['street']:
                street_entity_id = self.create_or_get_street_entity(
                    row, city_id, city_district_id, street_type_id
                )

                # Будівля з приміщеннями
                if row['building'] or row['flat'] or row['room']:
                    building_id = self.create_building_with_premises(row, street_entity_id)
                else:
                    # Лише вулиця
                    building_id = None
            else:
                # Може бути місто/район без вулиці
                street_entity_id = None
                building_id = None

            # Збереження зв'язку з джерелом
            original_data = {
                'id': int(row['id']) if row['id'] else None,
                'path': str(row['path']) if row['path'] else None,
                'city': row['city'],
                'street': row['street'],
                'building': row['building'],
                'flat': row['flat'],
                'room': row['room']
            }

            object_type = 'premise' if row['flat'] or row['room'] else 'building' if row['building'] else 'street'
            object_id = building_id or street_entity_id or city_id

            self.cursor.execute("""
                INSERT INTO addrinity.object_sources
                (object_type, object_id, source_id, original_data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (object_type, object_id, source_id, json.dumps(original_data, ensure_ascii=False)))

            self.stats['processed'] += 1

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
            """)

            if dry_run:
                self.logger.info("Тестовий запуск завершено (дані не збережено)")

        except Exception as e:
            self.logger.error(f"Критична помилка міграції: {e}")
            raise
        finally:
            self.connection.close()


