"""Мігратор для таблиці addr.ek_addr"""

import pandas as pd
import psycopg2
import json
from tqdm import tqdm
from src.utils.logger import migration_logger
from config.database import CONNECTION_STRING

class EkAddrMigrator:
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
            """, ('ek_addr', 'Таблиця адрес ЕК (addr.ek_addr)'))
            self.connection.commit()
            self.logger.info("Джерело ek_addr зареєстровано")
        except Exception as e:
            self.logger.error(f"Помилка налаштування джерела: {e}")

    def get_source_id(self):
        """Отримання ID джерела"""
        self.cursor.execute("SELECT id FROM addrinity.data_sources WHERE name = 'ek_addr'")
        return self.cursor.fetchone()[0]

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
            # Спочатку шукаємо існуюче місто
            self.cursor.execute("""
                SELECT id FROM addrinity.cities
                WHERE name_uk = %s
                LIMIT 1
            """, (city_name,))

            result = self.cursor.fetchone()
            if result:
                return result[0]

            # Якщо немає - створюємо (спрощена ієрархія)
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
            raise

    def get_or_create_district_for_ek(self, city_id, district_name):
        """Отримання або створення району міста для ek_addr"""
        if not district_name:
            return None

        try:
            self.cursor.execute("""
                INSERT INTO addrinity.city_districts
                (city_id, name_uk, type)
                VALUES (%s, %s, %s)
                ON CONFLICT (city_id, name_uk) DO UPDATE SET
                    type = EXCLUDED.type
                RETURNING id
            """, (city_id, district_name, 'адміністративний'))

            district_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return district_id
        except Exception as e:
            self.connection.rollback()
            raise

    def get_or_create_street_type(self, type_name):
        """Отримання або створення типу вулиці"""
        if not type_name:
            type_name = 'вулиця'

        try:
            self.cursor.execute("""
                INSERT INTO addrinity.street_types (name_uk, short_name_uk, ek_addr_type_code)
                VALUES (%s, %s, %s)
                ON CONFLICT (ek_addr_type_code) DO UPDATE SET
                    name_uk = EXCLUDED.name_uk
                RETURNING id
            """, (type_name, self.get_short_name(type_name), type_name))

            type_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return type_id
        except Exception as e:
            self.connection.rollback()
            raise

    def get_short_name(self, full_name):
        """Отримання скороченої назви типу вулиці"""
        short_names = {
            'вулиця': 'вул.',
            'проспект': 'просп.',
            'бульвар': 'бул.',
            'шосе': 'ш.',
            'тупик': 'туп.',
            'провулок': 'пров.',
            'майдан': 'майд.',
            'житловий масив': 'ж/м'
        }
        return short_names.get(full_name.lower(), full_name)

    def create_or_get_street_entity(self, row, city_id, district_id, street_type_id):
        """Створення або отримання вуличного об'єкта"""
        try:
            street_key = f"ek_{row['street']}_{row['street_type']}"

            # Перевірка наявності
            self.cursor.execute("""
                SELECT id FROM addrinity.street_entities
                WHERE ek_addr_street_key = %s
            """, (street_key,))

            result = self.cursor.fetchone()
            if result:
                return result[0]

            # Створення нового
            self.cursor.execute("""
                INSERT INTO addrinity.street_entities
                (city_id, city_district_id, type_id, ek_addr_street_key)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (city_id, district_id, street_type_id, street_key))

            street_entity_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return street_entity_id
        except Exception as e:
            self.connection.rollback()
            raise

    def create_building_with_premises(self, row, street_entity_id, ek_addr_key):
        """Створення будівлі з квартирами/приміщеннями"""
        try:
            # Створення будівлі
            building_number = row['build'] or ''
            corpus = row['corp'] or None

            self.cursor.execute("""
                INSERT INTO addrinity.buildings
                (street_entity_id, number, corpus, ek_addr_building_key)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ek_addr_building_key) DO UPDATE SET
                    street_entity_id = EXCLUDED.street_entity_id
                RETURNING id
            """, (street_entity_id, building_number, corpus, ek_addr_key))

            building_id = self.cursor.fetchone()[0]

            # Створення приміщення (якщо є квартира)
            if row['flat']:
                self.cursor.execute("""
                    INSERT INTO addrinity.premises
                    (building_id, number, type, ek_addr_premise_key)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (building_id, row['flat'], 'квартира', f"{ek_addr_key}_{row['flat']}"))

            self.connection.commit()
            return building_id
        except Exception as e:
            self.connection.rollback()
            raise

    def process_single_row(self, row, source_id):
        """Обробка одного рядка з ek_addr"""
        try:
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
            if row['build']:
                building_id = self.create_building_with_premises(row, street_entity_id, ek_addr_key)
            else:
                building_id = None

            # Збереження зв'язку з джерелом
            original_data = {
                'district': row['district'],
                'street_type': row['street_type'],
                'street': row['street'],
                'build': row['build'],
                'corp': row['corp'],
                'flat': row['flat']
            }

            object_type = 'premise' if row['flat'] else 'building' if row['build'] else 'street'
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
            """, self.connection)

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
                        self.stats['processed'] += 1

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
            """)

            if dry_run:
                self.logger.info("Тестовий запуск завершено (дані не збережено)")

        except Exception as e:
            self.logger.error(f"Критична помилка міграції: {e}")
            raise
        finally:
            self.connection.close()


