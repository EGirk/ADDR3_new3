"""Пошук адрес по вільному тексту з використанням валідатора"""

from src.utils.validators import get_universal_comparator
import psycopg2
import json
from config.database import CONNECTION_STRING

class AddressSearchEngine:
    def __init__(self):
        self.connection = psycopg2.connect(CONNECTION_STRING)
        self.cursor = self.connection.cursor()
        self.comparator = get_universal_comparator()
    
    def search_by_free_text(self, query_text, limit=50):
        """
        Пошук адрес по вільному тексту
        Приклади запитів:
        - "вулиця Хрещатик 15"
        - "Дніпро, Старий Шлях 192"
        - "Кірова вул., буд. 100"
        - "Таромський район, Золотоосіння 117"
        """
        results = {
            'query': query_text,
            'matches': [],
            'total_found': 0,
            'suggestions': []
        }
        
        try:
            # Парсинг вільного тексту
            parsed_query = self.parse_free_text(query_text)
            
            # Пошук за різними критеріями
            if parsed_query.get('street') and parsed_query.get('building'):
                # Пошук конкретної будівлі
                matches = self.search_building(
                    parsed_query['street'], 
                    parsed_query['building'],
                    parsed_query.get('city')
                )
            elif parsed_query.get('street'):
                # Пошук вулиці
                matches = self.search_street(
                    parsed_query['street'],
                    parsed_query.get('city'),
                    parsed_query.get('district')
                )
            elif parsed_query.get('district'):
                # Пошук району
                matches = self.search_district(
                    parsed_query['district'],
                    parsed_query.get('city')
                )
            else:
                # Загальний пошук
                matches = self.search_general(query_text)
            
            results['matches'] = matches
            results['total_found'] = len(matches)
            
            # Генерація підказок
            if len(matches) == 0:
                results['suggestions'] = self.generate_suggestions(query_text)
            
        except Exception as e:
            results['error'] = str(e)
        
        return results
    
    def parse_free_text(self, text):
        """Парсинг вільного тексту в структурований запит"""
        parsed = {
            'original': text,
            'parts': [],
            'street': None,
            'building': None,
            'district': None,
            'city': 'Дніпро',  # За замовчуванням
            'street_type': None
        }
        
        # Нормалізація тексту
        text = text.strip().lower()
        
        # Словники для розпізнавання
        street_types = {
            'вул', 'вулиця', 'просп', 'проспект', 'бул', 'бульвар',
            'пров', 'провулок', 'шосе', 'туп', 'тупик', 'майд', 'майдан'
        }
        
        district_indicators = {
            'район', 'р-н', 'мікрорайон', 'мкр', 'житловий масив'
        }
        
        building_indicators = {
            'буд', 'будинок', 'дім', 'д', '№'
        }
        
        # Розділення на частини
        parts = text.replace(',', ' ').replace('.', ' ').split()
        parsed['parts'] = parts
        
        # Пошук номера будинку
        for i, part in enumerate(parts):
            if any(ind in part for ind in building_indicators) and i + 1 < len(parts):
                parsed['building'] = parts[i + 1]
                break
            elif part.isdigit() and len(part) <= 4:
                parsed['building'] = part
        
        # Пошук типу вулиці
        for part in parts:
            if any(st in part for st in street_types):
                parsed['street_type'] = part
                break
        
        # Пошук району
        for part in parts:
            if any(di in part for di in district_indicators):
                # Знайти назву району
                for p in parts:
                    if p not in street_types and p not in district_indicators and not p.isdigit():
                        parsed['district'] = p.title()
                        break
                break
        
        # Пошук назви вулиці (все, що не є типом, номером чи районом)
        street_parts = []
        for part in parts:
            if (part not in street_types and 
                part not in district_indicators and 
                part not in building_indicators and 
                not part.isdigit() and
                part not in ['дніпро', 'м', 'м.']):
                street_parts.append(part.title())
        
        if street_parts:
            parsed['street'] = ' '.join(street_parts)
        
        return parsed
    
    def search_building(self, street_name, building_number, city=None):
        """Пошук конкретної будівлі"""
        matches = []
        
        try:
            # Пошук вулиці
            self.cursor.execute("""
                SELECT se.id, se.city_id, sn.name as street_name
                FROM addrinity.street_entities se
                JOIN addrinity.street_names sn ON se.id = sn.street_entity_id
                WHERE similarity(sn.name, %s) > 0.7 
                AND sn.is_current = TRUE
                ORDER BY similarity(sn.name, %s) DESC
                LIMIT 10
            """, (street_name, street_name))
            
            streets = self.cursor.fetchall()
            
            for street_id, city_id, found_street_name in streets:
                # Пошук будівлі на цій вулиці
                self.cursor.execute("""
                    SELECT b.id, b.number, b.corpus, c.name_uk as city_name
                    FROM addrinity.buildings b
                    JOIN addrinity.cities c ON c.id = (
                        SELECT city_id FROM addrinity.street_entities WHERE id = %s
                    )
                    WHERE b.street_entity_id = %s 
                    AND (b.number = %s OR similarity(b.number, %s) > 0.8)
                    LIMIT 5
                """, (street_id, street_id, building_number, building_number))
                
                buildings = self.cursor.fetchall()
                
                for building_id, number, corpus, city_name in buildings:
                    # Отримання повної адреси
                    full_address = self.get_full_address(street_id, building_id)
                    
                    matches.append({
                        'type': 'building',
                        'id': building_id,
                        'street': found_street_name,
                        'building': number,
                        'corpus': corpus,
                        'city': city_name,
                        'full_address': full_address,
                        'confidence': self.calculate_address_confidence(
                            street_name, found_street_name, 
                            building_number, number
                        )
                    })
        
        except Exception as e:
            pass
        
        return sorted(matches, key=lambda x: x['confidence'], reverse=True)
    
    def search_street(self, street_name, city=None, district=None):
        """Пошук вулиці"""
        matches = []
        
        try:
            # Пошук схожих вулиць
            similar_streets = self.comparator.find_similar_objects_universal(
                street_name, 'street', 0.6
            )
            
            for street_name_match, similarity_score in similar_streets[:20]:
                # Отримання інформації про вулицю
                self.cursor.execute("""
                    SELECT se.id, se.city_id, st.name_uk as street_type,
                           c.name_uk as city_name
                    FROM addrinity.street_entities se
                    JOIN addrinity.street_names sn ON se.id = sn.street_entity_id
                    JOIN addrinity.street_types st ON se.type_id = st.id
                    JOIN addrinity.cities c ON se.city_id = c.id
                    WHERE sn.name = %s AND sn.is_current = TRUE
                    LIMIT 1
                """, (street_name_match,))
                
                street_info = self.cursor.fetchone()
                if street_info:
                    street_id, city_id, street_type, city_name = street_info
                    
                    # Отримання будівель на вулиці
                    self.cursor.execute("""
                        SELECT id, number, corpus 
                        FROM addrinity.buildings 
                        WHERE street_entity_id = %s
                        ORDER BY number
                        LIMIT 10
                    """, (street_id,))
                    
                    buildings = self.cursor.fetchall()
                    
                    matches.append({
                        'type': 'street',
                        'id': street_id,
                        'name': street_name_match,
                        'type_name': street_type,
                        'city': city_name,
                        'buildings_count': len(buildings),
                        'sample_buildings': [b[1] for b in buildings[:5]],
                        'confidence': similarity_score
                    })
        
        except Exception as e:
            pass
        
        return sorted(matches, key=lambda x: x['confidence'], reverse=True)
    
    def search_district(self, district_name, city=None):
        """Пошук району"""
        matches = []
        
        try:
            # Пошук схожих районів
            similar_districts = self.comparator.find_similar_objects_universal(
                district_name, 'district', 0.7
            )
            
            for district_name_match, similarity_score in similar_districts[:10]:
                # Отримання інформації про район
                self.cursor.execute("""
                    SELECT id, name_uk, type, city_id
                    FROM addrinity.city_districts 
                    WHERE name_uk = %s
                    LIMIT 1
                """, (district_name_match,))
                
                district_info = self.cursor.fetchone()
                if district_info:
                    district_id, name, district_type, city_id = district_info
                    
                    # Отримання міста
                    self.cursor.execute("""
                        SELECT name_uk FROM addrinity.cities WHERE id = %s
                    """, (city_id,))
                    city_name = self.cursor.fetchone()[0] if self.cursor.fetchone() else "Невідомо"
                    
                    # Отримання вулиць у районі
                    self.cursor.execute("""
                        SELECT COUNT(*) FROM addrinity.street_entities 
                        WHERE city_district_id = %s
                    """, (district_id,))
                    streets_count = self.cursor.fetchone()[0]
                    
                    matches.append({
                        'type': 'district',
                        'id': district_id,
                        'name': name,
                        'type_name': district_type,
                        'city': city_name,
                        'streets_count': streets_count,
                        'confidence': similarity_score
                    })
        
        except Exception as e:
            pass
        
        return sorted(matches, key=lambda x: x['confidence'], reverse=True)
    
    def search_general(self, query_text):
        """Загальний пошук"""
        matches = []
        
        # Пошук по всіх типах об'єктів
        object_types = ['street', 'district', 'city']
        
        for obj_type in object_types:
            try:
                similar_objects = self.comparator.find_similar_objects_universal(
                    query_text, obj_type, 0.6
                )
                
                for obj_name, similarity_score in similar_objects[:5]:
                    matches.append({
                        'type': obj_type,
                        'name': obj_name,
                        'confidence': similarity_score,
                        'search_term': query_text
                    })
            except:
                continue
        
        return sorted(matches, key=lambda x: x['confidence'], reverse=True)
    
    def get_full_address(self, street_id, building_id=None):
        """Отримання повної адреси"""
        try:
            if building_id:
                self.cursor.execute("""
                    SELECT c.name_uk, cd.name_uk, sn.name, st.short_name_uk, b.number, b.corpus
                    FROM addrinity.buildings b
                    JOIN addrinity.street_entities se ON b.street_entity_id = se.id
                    JOIN addrinity.street_names sn ON se.id = sn.street_entity_id AND sn.is_current = TRUE
                    JOIN addrinity.street_types st ON se.type_id = st.id
                    JOIN addrinity.cities c ON se.city_id = c.id
                    LEFT JOIN addrinity.city_districts cd ON se.city_district_id = cd.id
                    WHERE b.id = %s AND sn.is_current = TRUE
                """, (building_id,))
                
                result = self.cursor.fetchone()
                if result:
                    city, district, street, street_type, number, corpus = result
                    corpus_part = f"/{corpus}" if corpus else ""
                    return f"{city}, {street} {street_type}, {number}{corpus_part}"
            
            # Якщо немає будівлі, повертаємо адресу вулиці
            self.cursor.execute("""
                SELECT c.name_uk, cd.name_uk, sn.name, st.short_name_uk
                FROM addrinity.street_entities se
                JOIN addrinity.street_names sn ON se.id = sn.street_entity_id AND sn.is_current = TRUE
                JOIN addrinity.street_types st ON se.type_id = st.id
                JOIN addrinity.cities c ON se.city_id = c.id
                LEFT JOIN addrinity.city_districts cd ON se.city_district_id = cd.id
                WHERE se.id = %s
            """, (street_id,))
            
            result = self.cursor.fetchone()
            if result:
                city, district, street, street_type = result
                return f"{city}, {street} {street_type}"
                
        except Exception as e:
            return "Адреса не знайдена"
        
        return "Адреса не знайдена"
    
    def calculate_address_confidence(self, search_street, found_street, search_building, found_building):
        """Розрахунок рівня довіри для адреси"""
        street_similarity = self.comparator.calculate_comprehensive_similarity(
            search_street, found_street, 'street'
        )
        
        building_similarity = 1.0 if search_building == found_building else 0.5
        
        return (street_similarity * 0.7 + building_similarity * 0.3)
    
    def generate_suggestions(self, query_text):
        """Генерація підказок для пошуку"""
        suggestions = []
        
        # Пошук схожих об'єктів
        for obj_type in ['street', 'district']:
            try:
                similar = self.comparator.find_similar_objects_universal(
                    query_text, obj_type, 0.5
                )
                for name, score in similar[:3]:
                    suggestions.append({
                        'type': obj_type,
                        'name': name,
                        'confidence': score,
                        'suggestion': f"Можливо ви мали на увазі: {name}?"
                    })
            except:
                continue
        
        return suggestions
    
    def fuzzy_search(self, partial_text, object_type='street', limit=10):
        """Нечіткий пошук з автодоповненням"""
        try:
            # Використання fuzzywuzzy для автодоповнення
            if object_type == 'street':
                self.cursor.execute("""
                    SELECT DISTINCT name FROM addrinity.street_names 
                    WHERE is_current = TRUE AND name IS NOT NULL
                """)
            elif object_type == 'district':
                self.cursor.execute("""
                    SELECT DISTINCT name_uk FROM addrinity.city_districts 
                    WHERE name_uk IS NOT NULL
                """)
            
            all_objects = [row[0] for row in self.cursor.fetchall() if row[0]]
            
            from fuzzywuzzy import process
            matches = process.extract(partial_text, all_objects, limit=limit)
            
            return [{'name': name, 'score': score} for name, score in matches if score > 30]
            
        except Exception as e:
            return []
    
    def close(self):
        """Закриття з'єднання"""
        if self.connection:
            self.connection.close()

# Глобальний екземпляр для зручності
search_engine = None

def get_search_engine():
    """Отримання глобального екземпляра пошукової системи"""
    global search_engine
    if not search_engine:
        search_engine = AddressSearchEngine()
    return search_engine

# Приклад використання
if __name__ == "__main__":
    # Тест пошуку
    searcher = get_search_engine()
    
    test_queries = [
        "вулиця Хрещатик 15",
        "Дніпро, Старий Шлях 192", 
        "Кірова вул., буд. 100",
        "Таромський район",
        "Золотоосіння"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Пошук: '{query}'")
        results = searcher.search_by_free_text(query)
        print(f"Знайдено: {results['total_found']}")
        for match in results['matches'][:3]:
            print(f"  - {match.get('full_address', match.get('name', 'Невідомо'))}")

            