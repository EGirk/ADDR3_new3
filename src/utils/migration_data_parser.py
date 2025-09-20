"""Утиліта для парсингу міграційних даних з файлу migrations/DATA-TrinitY-3.txt"""

import csv
import re
from io import StringIO
from pathlib import Path


class MigrationDataParser:
    """Клас для парсингу різних форматів міграційних даних"""
    
    def __init__(self, file_path: str = None):
        self.file_path = file_path or "/home/runner/work/ADDR3_new3/ADDR3_new3/migrations/DATA-TrinitY-3.txt"
        
    def parse_rtg_addr_section(self):
        """Парсинг секції addr.rtg_addr з файлу"""
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Знаходження секції rtg_addr
            rtg_start = content.find('-----------   таблиця =   addr.rtg_addr;')
            if rtg_start == -1:
                raise ValueError("Секція addr.rtg_addr не знайдена")
            
            # Знаходження кінця секції (до наступної таблиці або кінця файлу)
            rtg_end = content.find('------------------  Таблиця   bld_local', rtg_start)
            if rtg_end == -1:
                rtg_end = len(content)
            
            rtg_section = content[rtg_start:rtg_end].strip()
            
            # Розділення на рядки
            lines = rtg_section.split('\n')
            
            # Знаходження заголовків
            header_line = None
            data_start_index = 0
            
            for i, line in enumerate(lines):
                if 'id|path|tech_status|region|district' in line:
                    header_line = line
                    data_start_index = i + 1
                    break
            
            if not header_line:
                raise ValueError("Заголовки колонок не знайдені")
            
            # Парсинг заголовків
            headers = header_line.split('|')
            
            # Парсинг даних
            records = []
            for line in lines[data_start_index:]:
                line = line.strip()
                if not line or line.startswith('-') or line.startswith(' '):
                    continue
                    
                # Розділення по |
                values = line.split('|')
                
                # Перевірка відповідності кількості колонок
                if len(values) != len(headers):
                    continue
                
                # Створення словника запису
                record = {}
                for i, header in enumerate(headers):
                    value = values[i].strip()
                    # Обробка [NULL] значень
                    if value == '[NULL]' or value == '':
                        value = None
                    record[header] = value
                
                records.append(record)
            
            return records
            
        except Exception as e:
            raise Exception(f"Помилка парсингу rtg_addr секції: {e}")
    
    def normalize_record(self, record):
        """Нормалізація запису rtg_addr"""
        
        # Нормалізація основних полів
        normalized = {}
        
        # ID та path - ID може мати пробіли та non-breaking spaces, тому очищуємо
        id_str = record['id'].replace(' ', '').replace('\xa0', '') if record['id'] else None
        normalized['id'] = int(id_str) if id_str and id_str.isdigit() else None
        normalized['path'] = record['path']
        normalized['tech_status'] = int(record['tech_status']) if record['tech_status'] and record['tech_status'].isdigit() else 0
        
        # Географічні поля
        normalized['region'] = self._clean_text(record['region'])
        normalized['district'] = self._clean_text(record['district'])
        normalized['community'] = self._clean_text(record['community'])
        normalized['city'] = self._clean_text(record['city'])
        normalized['city_district'] = self._clean_text(record['city_district'])
        
        # Тип населеного пункту
        normalized['city_type'] = self._clean_text(record['city_type'])
        
        # Вулиці
        normalized['street'] = self._clean_text(record['street'])
        normalized['street_type'] = self._clean_text(record['street_type'])
        normalized['street_old'] = self._clean_text(record['street_old'])
        
        # Будівлі та приміщення
        normalized['building'] = self._clean_text(record['building'])
        normalized['corp'] = self._clean_text(record['corp'])
        normalized['flat'] = self._clean_text(record['flat'])
        normalized['room'] = self._clean_text(record['room'])
        
        # Типи
        normalized['build_type_id'] = int(record['build_type_id']) if record['build_type_id'] and record['build_type_id'].isdigit() else None
        normalized['prem_type'] = self._clean_text(record['prem_type'])
        normalized['apartment_type_id'] = int(record['apartment_type_id']) if record['apartment_type_id'] and record['apartment_type_id'].isdigit() else None
        
        # Дати та користувачі
        normalized['date_created'] = record['date_created']
        normalized['date_modified'] = record['date_modified'] 
        normalized['last_modified_by'] = record['last_modified_by']
        normalized['owner_id'] = int(record['owner_id']) if record['owner_id'] and record['owner_id'].isdigit() else None
        
        return normalized
    
    def _clean_text(self, text):
        """Очищення тексту"""
        if not text or text == '[NULL]':
            return None
        
        # Видалення зайвих пробілів
        text = text.strip()
        
        # Видалення подвійних пробілів
        text = re.sub(r'\s+', ' ', text)
        
        return text if text else None
    
    def parse_path_hierarchy(self, path_str):
        """Парсинг ієрархії з path"""
        if not path_str:
            return {}
        
        # Розділення path на частини
        parts = str(path_str).split('.')
        
        # Основна ієрархія адмінтериторій
        hierarchy = {
            'country': parts[0] if len(parts) > 0 else None,
            'region': parts[1] if len(parts) > 1 else None,
            'district': parts[2] if len(parts) > 2 else None,
            'community': parts[3] if len(parts) > 3 else None,
            'city': parts[4] if len(parts) > 4 else None,
        }
        
        # Додаткові рівні для об'єктів
        if len(parts) > 5:
            hierarchy['object_level_1'] = parts[5]
        if len(parts) > 6:
            hierarchy['object_level_2'] = parts[6]
        if len(parts) > 7:
            hierarchy['object_level_3'] = parts[7]
            
        return hierarchy
    
    def get_statistics(self, records):
        """Статистика по записам"""
        if not records:
            return {}
        
        stats = {
            'total_records': len(records),
            'with_streets': len([r for r in records if r.get('street')]),
            'with_buildings': len([r for r in records if r.get('building')]),
            'with_apartments': len([r for r in records if r.get('flat')]),
            'with_rooms': len([r for r in records if r.get('room')]),
            'unique_regions': len(set(r.get('region') for r in records if r.get('region'))),
            'unique_cities': len(set(r.get('city') for r in records if r.get('city'))),
            'unique_streets': len(set(r.get('street') for r in records if r.get('street'))),
        }
        
        return stats


def test_parser():
    """Тестування парсера"""
    print("Тестування парсера міграційних даних...")
    
    try:
        parser = MigrationDataParser()
        records = parser.parse_rtg_addr_section()
        
        print(f"Успішно зпарсено {len(records)} записів")
        
        if records:
            # Показати перший запис
            print("\nПерший запис:")
            first_record = parser.normalize_record(records[0])
            for key, value in first_record.items():
                print(f"  {key}: {value}")
            
            # Статистика
            stats = parser.get_statistics([parser.normalize_record(r) for r in records])
            print(f"\nСтатистика:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
                
    except Exception as e:
        print(f"Помилка тестування: {e}")


if __name__ == "__main__":
    test_parser()