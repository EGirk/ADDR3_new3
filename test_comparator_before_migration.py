#!/usr/bin/env python3
"""Тест компаратора ДО міграції (на основі вхідних даних)"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.validators import get_universal_comparator
import psycopg2
from config.database import SQLALCHEMY_URL
import pandas as pd
from sqlalchemy import create_engine

def test_comparator_before_migration():
    """Тест компаратора на основі вхідних даних"""
    
    # Використовуємо SQLAlchemy для уникнення попереджень
    engine = create_engine(SQLALCHEMY_URL)
    comparator = get_universal_comparator()
    
    print("🔍 ТЕСТ КОМПАРАТОРА ДО МІГРАЦІЇ")
    print("=" * 50)
    
    # 1. Аналіз даних з bld_local
    print("📋 АНАЛІЗ bld_local:")
    try:
        df_bld = pd.read_sql("""
            SELECT DISTINCT 
                street_ukr,
                raion,
                type_ukr,
                adres_n_uk,
                adres_o_uk
            FROM addr.bld_local 
            WHERE street_ukr IS NOT NULL 
            LIMIT 20
        """, engine)
        
        print(f"Знайдено {len(df_bld)} унікальних записів у bld_local")
        
        # Показуємо деякі дані
        if len(df_bld) > 0:
            print("\n📊 Приклади даних з bld_local:")
            for i, row in df_bld.head(3).iterrows():
                print(f"  Вулиця: {row['street_ukr']}")
                print(f"  Район: {row['raion']}")
                print(f"  Нова адреса: {row['adres_n_uk']}")
                print(f"  Стара адреса: {row['adres_o_uk']}")
                print()
        
    except Exception as e:
        print(f"Помилка аналізу bld_local: {e}")
    
    # 2. Аналіз даних з ek_addr
    print("📋 АНАЛІЗ ek_addr:")
    try:
        df_ek = pd.read_sql("""
            SELECT DISTINCT 
                district,
                street_type,
                street,
                build
            FROM addr.ek_addr 
            WHERE street IS NOT NULL 
            LIMIT 10
        """, engine)
        
        print(f"Знайдено {len(df_ek)} унікальних записів у ek_addr")
        
        if len(df_ek) > 0:
            print("\n📊 Приклади даних з ek_addr:")
            for i, row in df_ek.head(3).iterrows():
                print(f"  Район: {row['district']}")
                print(f"  Тип: {row['street_type']}")
                print(f"  Вулиця: {row['street']}")
                print(f"  Будинок: {row['build']}")
                print()
        
    except Exception as e:
        print(f"Помилка аналізу ek_addr: {e}")
    
    # 3. Аналіз даних з rtg_addr
    print("📋 АНАЛІЗ rtg_addr:")
    try:
        df_rtg = pd.read_sql("""
            SELECT DISTINCT 
                city,
                city_district,
                street_type,
                street,
                building
            FROM addr.rtg_addr 
            WHERE street IS NOT NULL AND city = 'Дніпро'
            LIMIT 10
        """, engine)
        
        print(f"Знайдено {len(df_rtg)} унікальних записів у rtg_addr (Дніпро)")
        
        if len(df_rtg) > 0:
            print("\n📊 Приклади даних з rtg_addr:")
            for i, row in df_rtg.head(3).iterrows():
                print(f"  Місто: {row['city']}")
                print(f"  Район: {row['city_district']}")
                print(f"  Тип: {row['street_type']}")
                print(f"  Вулиця: {row['street']}")
                print(f"  Будинок: {row['building']}")
                print()
        
    except Exception as e:
        print(f"Помилка аналізу rtg_addr: {e}")
    
    # 4. Тест компаратора на реальних даних
    print("🧪 ТЕСТ КОМПАРАТОРА:")
    
    # Збираємо реальні дані для тестування
    test_data = []
    
    # З bld_local
    if len(df_bld) > 0:
        streets = df_bld['street_ukr'].dropna().unique()[:5]
        test_data.extend([(street, "street") for street in streets])
    
    # З ek_addr
    if len(df_ek) > 0:
        districts = df_ek['district'].dropna().unique()[:3]
        test_data.extend([(district, "district") for district in districts])
        
        street_types = df_ek['street_type'].dropna().unique()[:3]
        test_data.extend([(st_type, "street_type") for st_type in street_types])
    
    # З rtg_addr
    if len(df_rtg) > 0:
        city_districts = df_rtg['city_district'].dropna().unique()[:3]
        test_data.extend([(district, "district") for district in city_districts])
    
    print(f"\nВсього об'єктів для тестування: {len(test_data)}")
    
    # Тестуємо кожен об'єкт
    for i, (name, obj_type) in enumerate(test_data[:10]):  # Тільки перші 10
        print(f"\n--- Тест {i+1}: '{name}' ({obj_type}) ---")
        
        # Нормалізація
        normalized = comparator.normalize_text(str(name), obj_type)
        print(f"Нормалізовано: '{normalized}'")
        
        # Валідація (поки що без порівняння з існуючими)
        validation = comparator.validate_object_universally(str(name), obj_type)
        print(f"Рівень довіри: {validation['confidence_level']}")
        print(f"Рекомендація: {validation['recommendation']}")
    
    # 5. Спеціальні тести з ваших даних
    print("\n🎯 СПЕЦІАЛЬНІ ТЕСТИ:")
    
    special_tests = [
        ("СТАРИЙ ШЛЯХ", "КІРОВА", "street"),
        ("ЗОЛОТООСІННЯ", "ЖОВТНЕВА", "street"),
        ("ВУЛ.", "вулиця", "street_type"),
        ("ТАРОМСКО", "Таромський", "district"),
        ("Тополя-2", "Тополя 2", "district")
    ]
    
    for name1, name2, obj_type in special_tests:
        print(f"\n--- '{name1}' vs '{name2}' ({obj_type}) ---")
        similarity = comparator.calculate_comprehensive_similarity(name1, name2, obj_type)
        print(f"Схожість: {similarity:.3f}")
        
        # Детальний аналіз
        validation1 = comparator.validate_object_universally(name1, obj_type)
        validation2 = comparator.validate_object_universally(name2, obj_type)
        print(f"'{name1}': {validation1['confidence_level']}")
        print(f"'{name2}': {validation2['confidence_level']}")
    
    engine.dispose()

if __name__ == "__main__":
    test_comparator_before_migration()