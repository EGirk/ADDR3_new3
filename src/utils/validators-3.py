"""Універсальний компаратор для всіх типів адресних об'єктів"""

import psycopg2
import re
from fuzzywuzzy import fuzz, process
import Levenshtein
import jellyfish
import textdistance
from config.database import CONNECTION_STRING

class UniversalAddressComparator:
    def __init__(self):
        self.connection = psycopg2.connect(CONNECTION_STRING)
        self.cursor = self.connection.cursor()
        self.setup_extensions()
    
    def setup_extensions(self):
        """Налаштування розширень PostgreSQL"""
        try:
            extensions = ['fuzzystrmatch', 'pg_trgm']
            for ext in extensions:
                self.cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
            self.connection.commit()
        except:
            pass
    
    def normalize_text(self, text, object_type=None):
        """Нормалізація тексту з урахуванням типу об'єкта"""
        if not text:
            return ""
        
        # Базова нормалізація
        text = re.sub(r'\s+', ' ', text.strip())
        text = text.replace('"', '').replace("'", "").replace('`', "'")
        
        # Специфічна нормалізація для різних типів
        if object_type == 'street_type':
            # Нормалізація типів вулиць
            street_type_mapping = {
                'вул.': 'вулиця', 'вул': 'вулиця',
                'просп.': 'проспект', 'просп': 'проспект',
                'бул.': 'бульвар', 'бул': 'бульвар',
                'пров.': 'провулок', 'пров': 'провулок',
                'ш.': 'шосе', 'ш': 'шосе',
                'туп.': 'тупик', 'туп': 'тупик'
            }
            text_lower = text.lower()
            for short, full in street_type_mapping.items():
                if text_lower == short or text_lower == full:
                    return full
        elif object_type == 'district':
            # Нормалізація районів
            text = re.sub(r'район$', '', text, flags=re.IGNORECASE).strip()
            text = re.sub(r'^м\.', '', text, flags=re.IGNORECASE).strip()
        
        return text.lower()
    
    def calculate_comprehensive_similarity(self, str1, str2, object_type=None):
        """
        Комплексний розрахунок схожості для будь-якого типу об'єкта
        """
        if not str1 or not str2:
            return 0.0
        
        # Нормалізація з урахуванням типу
        norm1 = self.normalize_text(str1, object_type)
        norm2 = self.normalize_text(str2, object_type)
        
        if not norm1 or not norm2:
            return 0.0
        
        # Специфічні ваги для різних типів об'єктів
        weights = {
            'street': {'exact': 0.30, 'fuzzy': 0.25, 'phonetic': 0.20, 'levenshtein': 0.15, 'jaro': 0.10},
            'district': {'exact': 0.40, 'fuzzy': 0.20, 'phonetic': 0.15, 'levenshtein': 0.15, 'jaro': 0.10},
            'street_type': {'exact': 0.50, 'fuzzy': 0.20, 'phonetic': 0.15, 'levenshtein': 0.10, 'jaro': 0.05},
            'city': {'exact': 0.35, 'fuzzy': 0.25, 'phonetic': 0.20, 'levenshtein': 0.10, 'jaro': 0.10},
            'building': {'exact': 0.60, 'fuzzy': 0.15, 'phonetic': 0.10, 'levenshtein': 0.10, 'jaro': 0.05},
            'default': {'exact': 0.30, 'fuzzy': 0.25, 'phonetic': 0.20, 'levenshtein': 0.15, 'jaro': 0.10}
        }
        
        current_weights = weights.get(object_type, weights['default'])
        
        similarities = {}
        
        # 1. Точне співпадіння (нормалізоване)
        exact_match = 1.0 if norm1 == norm2 else 0.0
        similarities['exact'] = exact_match
        
        # 2. fuzzywuzzy методи
        try:
            similarities['fuzz_ratio'] = fuzz.ratio(norm1, norm2) / 100.0
            similarities['fuzz_token_set'] = fuzz.token_set_ratio(norm1, norm2) / 100.0
            similarities['fuzz_token_sort'] = fuzz.token_sort_ratio(norm1, norm2) / 100.0
        except:
            similarities['fuzz_ratio'] = 0.0
            similarities['fuzz_token_set'] = 0.0
            similarities['fuzz_token_sort'] = 0.0
        
        # 3. Фонетична схожість (спеціально для української мови)
        phonetic_sim = self.calculate_ukrainian_phonetic_similarity(norm1, norm2)
        similarities['phonetic'] = phonetic_sim
        
        # 4. Levenshtein та Jaro-Winkler
        try:
            similarities['levenshtein'] = Levenshtein.ratio(norm1, norm2)
            similarities['jaro_winkler'] = Levenshtein.jaro_winkler(norm1, norm2)
        except:
            similarities['levenshtein'] = 0.0
            similarities['jaro_winkler'] = 0.0
        
        # 5. PostgreSQL similarity (якщо доступно)
        try:
            self.cursor.execute("SELECT similarity(%s, %s)", (norm1, norm2))
            similarities['pg_similarity'] = self.cursor.fetchone()[0]
        except:
            similarities['pg_similarity'] = similarities.get('fuzz_token_set', 0.0)
        
        # Комбінована оцінка з оптимальними вагами
        combined_score = (
            similarities.get('exact', 0.0) * current_weights['exact'] +
            similarities.get('fuzz_token_set', 0.0) * current_weights['fuzzy'] +
            similarities.get('phonetic', 0.0) * current_weights['phonetic'] +
            similarities.get('levenshtein', 0.0) * current_weights['levenshtein'] +
            similarities.get('jaro_winkler', 0.0) * current_weights['jaro']
        )
        
        return min(combined_score, 1.0)
    
    def calculate_ukrainian_phonetic_similarity(self, word1, word2):
        """Фонетична схожість для українських слів"""
        # Українські фонетичні заміни
        phonetic_replacements = {
            'і': 'и', 'ї': 'и', 'є': 'е', 'ґ': 'г',
            'й': 'и', 'ю': 'у', 'я': 'а',
            'кс': 'х', 'гз': 'з', 'дз': 'з',
            'тц': 'ц', 'дц': 'ц'
        }
        
        def apply_phonetic_rules(text):
            result = text.lower()
            for old, new in phonetic_replacements.items():
                result = result.replace(old, new)
            return result
        
        phonetic1 = apply_phonetic_rules(word1)
        phonetic2 = apply_phonetic_rules(word2)
        
        # Використовуємо стандартні методи для фонетичної форми
        try:
            return (fuzz.token_set_ratio(phonetic1, phonetic2) / 100.0 +
                   Levenshtein.ratio(phonetic1, phonetic2)) / 2.0
        except:
            return 0.0
    
    def find_similar_objects_universal(self, target_name, object_type, threshold=0.8):
        """
        Універсальний пошук схожих об'єктів будь-якого типу
        object_type: 'street', 'district', 'street_type', 'city', 'building'
        """
        if not target_name:
            return []
        
        # Запити для різних типів об'єктів
        queries = {
            'street': """
                SELECT DISTINCT name FROM addrinity.street_names 
                WHERE is_current = TRUE AND name IS NOT NULL
            """,
            'district': """
                SELECT DISTINCT name_uk FROM addrinity.city_districts 
                WHERE name_uk IS NOT NULL
                UNION
                SELECT DISTINCT name_uk FROM addrinity.districts 
                WHERE name_uk IS NOT NULL
            """,
            'street_type': """
                SELECT DISTINCT name_uk FROM addrinity.street_types 
                WHERE name_uk IS NOT NULL
                UNION
                SELECT DISTINCT short_name_uk FROM addrinity.street_types 
                WHERE short_name_uk IS NOT NULL
            """,
            'city': """
                SELECT DISTINCT name_uk FROM addrinity.cities 
                WHERE name_uk IS NOT NULL
            """,
            'building': """
                SELECT DISTINCT number FROM addrinity.buildings 
                WHERE number IS NOT NULL AND number != ''
            """
        }
        
        if object_type not in queries:
            return []
        
        try:
            self.cursor.execute(queries[object_type])
            existing_objects = [row[0] for row in self.cursor.fetchall() if row[0]]
        except Exception as e:
            return []
        
        # Використання fuzzywuzzy для швидкого пошуку
        if existing_objects:
            # Швидкий пошук топ-20 найсхожіших
            matches = process.extract(target_name, existing_objects, limit=20)
            # Фільтрація за порогом та додаткова перевірка
            final_matches = []
            for name, fuzzy_score in matches:
                if fuzzy_score/100.0 >= threshold * 0.7:  # менший поріг для fuzzywuzzy
                    our_score = self.calculate_comprehensive_similarity(
                        target_name, name, object_type
                    )
                    # Комбінуємо оцінки
                    combined_score = (fuzzy_score/100.0 * 0.6 + our_score * 0.4)
                    if combined_score >= threshold:
                        final_matches.append((name, combined_score))
            
            # Сортування
            final_matches.sort(key=lambda x: x[1], reverse=True)
            return final_matches[:20]
        
        return []
    
    def validate_object_universally(self, target_name, object_type):
        """
        Універсальна валідація будь-якого типу об'єкта
        """
        results = {
            'target_name': target_name,
            'object_type': object_type,
            'similar_objects': [],
            'confidence_level': 'low',
            'recommendation': 'create_new',
            'detailed_scores': {}
        }
        
        # Універсальний пошук
        similar_objects = self.find_similar_objects_universal(target_name, object_type, 0.75)
        results['similar_objects'] = similar_objects
        results['detailed_scores'] = {
            'total_found': len(similar_objects),
            'top_matches': similar_objects[:5] if similar_objects else []
        }
        
        if similar_objects:
            max_score = similar_objects[0][1]
            results['detailed_scores']['max_similarity'] = max_score
            
            # Визначення рівня довіри з урахуванням типу об'єкта
            thresholds = {
                'street': (0.95, 0.90, 0.85, 0.80),
                'district': (0.90, 0.85, 0.80, 0.75),
                'street_type': (0.95, 0.90, 0.85, 0.80),
                'city': (0.95, 0.90, 0.85, 0.80),
                'building': (0.98, 0.95, 0.90, 0.85),
                'default': (0.95, 0.90, 0.85, 0.80)
            }
            
            very_high, high, medium, low = thresholds.get(object_type, thresholds['default'])
            
            if max_score >= very_high:
                results['confidence_level'] = 'very_high'
                results['recommendation'] = 'use_existing'
            elif max_score >= high:
                results['confidence_level'] = 'high'
                results['recommendation'] = 'review'
            elif max_score >= medium:
                results['confidence_level'] = 'medium'
                results['recommendation'] = 'create_with_note'
            elif max_score >= low:
                results['confidence_level'] = 'low'
                results['recommendation'] = 'create_new_with_warning'
            else:
                results['confidence_level'] = 'very_low'
                results['recommendation'] = 'create_new'
        else:
            results['confidence_level'] = 'very_low'
            results['recommendation'] = 'create_new'
        
        return results

# Глобальний екземпляр
universal_comparator = UniversalAddressComparator()

def get_universal_comparator():
    """Отримання універсального компаратора"""
    return universal_comparator

