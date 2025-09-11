-- =================================================================================
-- СХЕМА addrinity - Універсальна адресна база даних
-- =================================================================================
CREATE SCHEMA IF NOT EXISTS addrinity;

-- =================================================================================
-- ТАБЛИЦЯ: Джерела даних
-- Призначення: Відстеження походження даних для забезпечення двобічної сумісності
-- =================================================================================
CREATE TABLE addrinity.data_sources (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE, -- Назва джерела: 'bld_local', 'ek_addr', 'rtg_addr'
    description TEXT -- Опис джерела даних
);

COMMENT ON TABLE addrinity.data_sources IS 'Джерела вихідних даних для міграції';
COMMENT ON COLUMN addrinity.data_sources.name IS 'Назва джерела даних (bld_local, ek_addr, rtg_addr)';
COMMENT ON COLUMN addrinity.data_sources.description IS 'Опис джерела даних';

-- =================================================================================
-- ТАБЛИЦЯ: Країни
-- Призначення: Зберігання країн з підтримкою оригінальних ідентифікаторів
-- =================================================================================
CREATE TABLE addrinity.countries (
    id SERIAL PRIMARY KEY,
    iso_code CHAR(2) UNIQUE NOT NULL, -- ISO 3166-1 alpha-2 код країни
    name_uk TEXT NOT NULL, -- Назва країни українською
    name_en TEXT, -- Назва країни англійською
    name_ru TEXT, -- Назва країни російською
    -- Оригінальні ідентифікатори з джерел
    rtg_country_id TEXT, -- ID країни з rtg_addr
    bld_local_country_code TEXT -- Код країни з bld_local (якщо є)
);

COMMENT ON TABLE addrinity.countries IS 'Країни з підтримкою оригінальних ідентифікаторів';
COMMENT ON COLUMN addrinity.countries.iso_code IS 'ISO 3166-1 alpha-2 код країни';
COMMENT ON COLUMN addrinity.countries.rtg_country_id IS 'ID країни з системи rtg_addr';
COMMENT ON COLUMN addrinity.countries.bld_local_country_code IS 'Код країни з системи bld_local';

-- =================================================================================
-- ТАБЛИЦЯ: Регіони/Області
-- Призначення: Адміністративні регіони з оригінальними кодами
-- =================================================================================
CREATE TABLE addrinity.regions (
    id SERIAL PRIMARY KEY,
    country_id INT REFERENCES addrinity.countries(id), -- Посилання на країну
    name_uk TEXT NOT NULL, -- Назва регіону українською
    code TEXT, -- Код регіону (якщо є)
    -- Оригінальні ідентифікатори
    rtg_region_id BIGINT, -- ID регіону з rtg_addr
    bld_local_region_key TEXT -- Унікальний ключ регіону з bld_local
);

COMMENT ON TABLE addrinity.regions IS 'Регіони/області з оригінальними ідентифікаторами';
COMMENT ON COLUMN addrinity.regions.rtg_region_id IS 'ID регіону з системи rtg_addr';
COMMENT ON COLUMN addrinity.regions.bld_local_region_key IS 'Унікальний ключ регіону з системи bld_local';

-- =================================================================================
-- ТАБЛИЦЯ: Райони (обласні/державні)
-- Призначення: Адміністративні райони з підтримкою оригінальних ID
-- =================================================================================
CREATE TABLE addrinity.districts (
    id SERIAL PRIMARY KEY,
    region_id INT REFERENCES addrinity.regions(id), -- Посилання на регіон
    name_uk TEXT NOT NULL, -- Назва району українською
    -- Оригінальні ідентифікатори
    rtg_district_id BIGINT, -- ID району з rtg_addr
    bld_local_district_key TEXT, -- Ключ району з bld_local
    ek_addr_district_key TEXT -- Ключ району з ek_addr
);

COMMENT ON TABLE addrinity.districts IS 'Адміністративні райони з оригінальними ідентифікаторами';
COMMENT ON COLUMN addrinity.districts.rtg_district_id IS 'ID району з системи rtg_addr';
COMMENT ON COLUMN addrinity.districts.bld_local_district_key IS 'Ключ району з системи bld_local';
COMMENT ON COLUMN addrinity.districts.ek_addr_district_key IS 'Ключ району з системи ek_addr';

-- =================================================================================
-- ТАБЛИЦЯ: Громади
-- Призначення: Територіальні громади з оригінальними ідентифікаторами
-- =================================================================================
CREATE TABLE addrinity.communities (
    id SERIAL PRIMARY KEY,
    district_id INT REFERENCES addrinity.districts(id), -- Посилання на район
    name_uk TEXT NOT NULL, -- Назва громади українською
    type TEXT, -- Тип громади: 'міська', 'сільська'
    -- Оригінальні ідентифікатори
    rtg_community_id BIGINT, -- ID громади з rtg_addr
    bld_local_community_key TEXT -- Ключ громади з bld_local
);

COMMENT ON TABLE addrinity.communities IS 'Територіальні громади з оригінальними ідентифікаторами';
COMMENT ON COLUMN addrinity.communities.type IS 'Тип громади: міська, сільська';
COMMENT ON COLUMN addrinity.communities.rtg_community_id IS 'ID громади з системи rtg_addr';
COMMENT ON COLUMN addrinity.communities.bld_local_community_key IS 'Ключ громади з системи bld_local';

-- =================================================================================
-- ТАБЛИЦЯ: Міста/селища
-- Призначення: Населені пункти з підтримкою оригінальних кодів
-- =================================================================================
CREATE TABLE addrinity.cities (
    id SERIAL PRIMARY KEY,
    community_id INT REFERENCES addrinity.communities(id), -- Посилання на громаду
    name_uk TEXT NOT NULL, -- Назва міста українською
    type TEXT, -- Тип населеного пункту: 'м.', 'с.', 'с-ще'
    -- Оригінальні ідентифікатори
    rtg_city_id BIGINT, -- ID міста з rtg_addr
    bld_local_city_key TEXT, -- Ключ міста з bld_local
    ek_addr_city_key TEXT -- Ключ міста з ek_addr
);

COMMENT ON TABLE addrinity.cities IS 'Населені пункти з оригінальними ідентифікаторами';
COMMENT ON COLUMN addrinity.cities.type IS 'Тип населеного пункту: м., с., с-ще';
COMMENT ON COLUMN addrinity.cities.rtg_city_id IS 'ID міста з системи rtg_addr';
COMMENT ON COLUMN addrinity.cities.bld_local_city_key IS 'Ключ міста з системи bld_local';
COMMENT ON COLUMN addrinity.cities.ek_addr_city_key IS 'Ключ міста з системи ek_addr';

-- =================================================================================
-- ТАБЛИЦЯ: Райони міст
-- Призначення: Адміністративні та історичні райони міст
-- =================================================================================
CREATE TABLE addrinity.city_districts (
    id SERIAL PRIMARY KEY,
    city_id INT REFERENCES addrinity.cities(id), -- Посилання на місто
    name_uk TEXT NOT NULL, -- Назва району міста українською
    type TEXT, -- Тип району: 'адміністративний', 'історичний', 'неформальний'
    -- Оригінальні ідентифікатори
    rtg_city_district_id BIGINT, -- ID району міста з rtg_addr
    bld_local_raion_name TEXT, -- Назва району з bld_local
    ek_addr_district_name TEXT -- Назва району з ek_addr
);

COMMENT ON TABLE addrinity.city_districts IS 'Райони міст (адміністративні, історичні, неформальні)';
COMMENT ON COLUMN addrinity.city_districts.type IS 'Тип району: адміністративний, історичний, неформальний';
COMMENT ON COLUMN addrinity.city_districts.rtg_city_district_id IS 'ID району міста з системи rtg_addr';
COMMENT ON COLUMN addrinity.city_districts.bld_local_raion_name IS 'Назва району з системи bld_local';
COMMENT ON COLUMN addrinity.city_districts.ek_addr_district_name IS 'Назва району з системи ek_addr';

-- =================================================================================
-- ТАБЛИЦЯ: Типи вулиць
-- Призначення: Класифікація типів вулиць з підтримкою оригінальних кодів
-- =================================================================================
CREATE TABLE addrinity.street_types (
    id SERIAL PRIMARY KEY,
    name_uk TEXT NOT NULL, -- Повна назва типу українською: 'вулиця', 'проспект'
    short_name_uk TEXT, -- Скорочена назва: 'вул.', 'просп.'
    -- Оригінальні коди
    rtg_type_code TEXT, -- Код типу з rtg_addr
    bld_local_type_code TEXT, -- Код типу з bld_local
    ek_addr_type_code TEXT -- Код типу з ek_addr
);

COMMENT ON TABLE addrinity.street_types IS 'Типи вулиць з оригінальними кодами';
COMMENT ON COLUMN addrinity.street_types.name_uk IS 'Повна назва типу вулиці українською';
COMMENT ON COLUMN addrinity.street_types.short_name_uk IS 'Скорочена назва типу вулиці';
COMMENT ON COLUMN addrinity.street_types.rtg_type_code IS 'Код типу вулиці з системи rtg_addr';
COMMENT ON COLUMN addrinity.street_types.bld_local_type_code IS 'Код типу вулиці з системи bld_local';
COMMENT ON COLUMN addrinity.street_types.ek_addr_type_code IS 'Код типу вулиці з системи ek_addr';

-- =================================================================================
-- ТАБЛИЦЯ: Вуличні об'єкти
-- Призначення: Абстрактні вуличні об'єкти з оригінальними ідентифікаторами
-- =================================================================================
CREATE TABLE addrinity.street_entities (
    id SERIAL PRIMARY KEY,
    city_id INT REFERENCES addrinity.cities(id), -- Посилання на місто
    city_district_id INT REFERENCES addrinity.city_districts(id), -- Посилання на район міста
    type_id INT REFERENCES addrinity.street_types(id), -- Посилання на тип вулиці
    -- Оригінальні ідентифікатори з різних джерел
    rtg_path TEXT UNIQUE, -- Ієрархічний шлях з rtg_addr (ltree)
    rtg_street_id BIGINT, -- ID вулиці з rtg_addr
    bld_local_objectid BIGINT, -- objectid з bld_local
    bld_local_id_street_rtg BIGINT, -- id_street_rtg з bld_local
    ek_addr_street_key TEXT -- Унікальний ключ вулиці з ek_addr
);

COMMENT ON TABLE addrinity.street_entities IS 'Абстрактні вуличні обєкти з оригінальними ідентифікаторами';
COMMENT ON COLUMN addrinity.street_entities.rtg_path IS 'Ієрархічний шлях з системи rtg_addr (ltree)';
COMMENT ON COLUMN addrinity.street_entities.rtg_street_id IS 'ID вулиці з системи rtg_addr';
COMMENT ON COLUMN addrinity.street_entities.bld_local_objectid IS 'objectid з системи bld_local';
COMMENT ON COLUMN addrinity.street_entities.bld_local_id_street_rtg IS 'id_street_rtg з системи bld_local';
COMMENT ON COLUMN addrinity.street_entities.ek_addr_street_key IS 'Унікальний ключ вулиці з системи ek_addr';

-- =================================================================================
-- ТАБЛИЦЯ: Назви вулиць
-- Призначення: Мовні та історичні варіанти назв вулиць
-- =================================================================================
CREATE TABLE addrinity.street_names (
    id SERIAL PRIMARY KEY,
    street_entity_id INT REFERENCES addrinity.street_entities(id), -- Посилання на вуличний об'єкт
    name TEXT NOT NULL, -- Назва вулиці
    language_code CHAR(2), -- Код мови: 'uk', 'ru', 'en'
    is_official BOOLEAN DEFAULT TRUE, -- Чи є офіційною назвою
    is_current BOOLEAN DEFAULT TRUE, -- Чи є поточною назвою
    name_type TEXT, -- Тип назви: 'current', 'old', 'alternative', 'historical'
    valid_from DATE, -- Дата початку дії назви
    valid_to DATE -- Дата закінчення дії назви
);

COMMENT ON TABLE addrinity.street_names IS 'Мовні та історичні варіанти назв вулиць';
COMMENT ON COLUMN addrinity.street_names.language_code IS 'Код мови: uk, ru, en';
COMMENT ON COLUMN addrinity.street_names.is_official IS 'Чи є офіційною назвою';
COMMENT ON COLUMN addrinity.street_names.is_current IS 'Чи є поточною назвою';
COMMENT ON COLUMN addrinity.street_names.name_type IS 'Тип назви: current, old, alternative, historical';
COMMENT ON COLUMN addrinity.street_names.valid_from IS 'Дата початку дії назви';
COMMENT ON COLUMN addrinity.street_names.valid_to IS 'Дата закінчення дії назви';

-- =================================================================================
-- ТАБЛИЦЯ: Будівлі
-- Призначення: Будівлі з оригінальними ідентифікаторами та характеристиками
-- =================================================================================
CREATE TABLE addrinity.buildings (
    id SERIAL PRIMARY KEY,
    street_entity_id INT REFERENCES addrinity.street_entities(id), -- Посилання на вулицю
    number TEXT NOT NULL, -- Номер будівлі (може містити букви: 12А, 12/3)
    corpus TEXT, -- Корпус будівлі
    postal_code TEXT, -- Поштовий індекс
    floors INT, -- Кількість поверхів
    entrances INT, -- Кількість під'їздів
    apartments INT, -- Кількість квартир
    latitude DECIMAL(10, 8), -- Широта
    longitude DECIMAL(11, 8), -- Довгота
    -- Оригінальні ідентифікатори
    rtg_building_id BIGINT, -- ID будівлі з rtg_addr
    bld_local_objectid BIGINT, -- objectid з bld_local
    bld_local_id_bld_rtg BIGINT, -- id_bld_rtg з bld_local
    ek_addr_building_key TEXT -- Унікальний ключ будівлі з ek_addr
);

COMMENT ON TABLE addrinity.buildings IS 'Будівлі з оригінальними ідентифікаторами та характеристиками';
COMMENT ON COLUMN addrinity.buildings.number IS 'Номер будівлі (може містити букви: 12А, 12/3)';
COMMENT ON COLUMN addrinity.buildings.corpus IS 'Корпус будівлі';
COMMENT ON COLUMN addrinity.buildings.floors IS 'Кількість поверхів';
COMMENT ON COLUMN addrinity.buildings.entrances IS 'Кількість підєздів';
COMMENT ON COLUMN addrinity.buildings.apartments IS 'Кількість квартир';
COMMENT ON COLUMN addrinity.buildings.rtg_building_id IS 'ID будівлі з системи rtg_addr';
COMMENT ON COLUMN addrinity.buildings.bld_local_objectid IS 'objectid з системи bld_local';
COMMENT ON COLUMN addrinity.buildings.bld_local_id_bld_rtg IS 'id_bld_rtg з системи bld_local';
COMMENT ON COLUMN addrinity.buildings.ek_addr_building_key IS 'Унікальний ключ будівлі з системи ek_addr';

-- =================================================================================
-- ТАБЛИЦЯ: Зв'язок об'єктів з джерелами
-- Призначення: Відстеження походження даних для кожного об'єкта
-- =================================================================================
CREATE TABLE addrinity.object_sources (
    id SERIAL PRIMARY KEY,
    object_type TEXT, -- Тип об'єкта: 'building', 'street', 'city', 'district'
    object_id INT, -- ID об'єкта в addrinity
    source_id INT REFERENCES addrinity.data_sources(id), -- Джерело даних
    original_data JSONB -- Оригінальні дані у форматі JSON
);

COMMENT ON TABLE addrinity.object_sources IS 'Звязок обєктів з джерелами даних для відстеження походження';
COMMENT ON COLUMN addrinity.object_sources.object_type IS 'Тип обєкта: building, street, city, district';
COMMENT ON COLUMN addrinity.object_sources.object_id IS 'ID обєкта в системі addrinity';
COMMENT ON COLUMN addrinity.object_sources.original_data IS 'Оригінальні дані у форматі JSON';

-- =================================================================================
-- ІНДЕКСИ ДЛЯ ОПТИМІЗАЦІЇ ПОШУКУ
-- =================================================================================
CREATE INDEX idx_countries_iso ON addrinity.countries(iso_code);
CREATE INDEX idx_regions_country ON addrinity.regions(country_id);
CREATE INDEX idx_districts_region ON addrinity.districts(region_id);
CREATE INDEX idx_communities_district ON addrinity.communities(district_id);
CREATE INDEX idx_cities_community ON addrinity.cities(community_id);
CREATE INDEX idx_city_districts_city ON addrinity.city_districts(city_id);
CREATE INDEX idx_street_entities_city ON addrinity.street_entities(city_id);
CREATE INDEX idx_street_entities_district ON addrinity.street_entities(city_district_id);
CREATE INDEX idx_street_entities_rtg_path ON addrinity.street_entities(rtg_path);
CREATE INDEX idx_street_entities_bld_objectid ON addrinity.street_entities(bld_local_objectid);
CREATE INDEX idx_street_entities_rtg_id ON addrinity.street_entities(rtg_street_id);
CREATE INDEX idx_street_names_entity ON addrinity.street_names(street_entity_id);
CREATE INDEX idx_street_names_current ON addrinity.street_names(is_current);
CREATE INDEX idx_buildings_street ON addrinity.buildings(street_entity_id);
CREATE INDEX idx_buildings_bld_objectid ON addrinity.buildings(bld_local_objectid);
CREATE INDEX idx_buildings_rtg_id ON addrinity.buildings(rtg_building_id);
CREATE INDEX idx_object_sources_type_id ON addrinity.object_sources(object_type, object_id);
CREATE INDEX idx_object_sources_source ON addrinity.object_sources(source_id);   

/* Qwen3-Coder  10:58 PM
🤦‍♂️ Ви абсолютно праві! Я пропустив квартири і не врахував повну структуру!
1. Квартири - додаємо 7-й рівень!📄 Оновлена структура addrinity.buildings:  */

-- Оновлюємо таблицю будівель
ALTER TABLE addrinity.buildings ADD COLUMN (
    building_key TEXT UNIQUE  -- Унікальний ключ будівлі для зв'язку з квартирами
);
     --   Висновок:   building_key був зайвим - унікальність уже забезпечена існуючими оригінальними ID! Дякую, що звернули увагу!
  --   --     Правильний синтаксис для PostgreSQL 11:
  /*  -- Додавання одного стовпця
ALTER TABLE addrinity.buildings ADD COLUMN building_key TEXT;
-- Додавання унікального обмеження окремо
ALTER TABLE addrinity.buildings ADD CONSTRAINT uniq_building_key UNIQUE (building_key);   */


-- =================================================================================
-- ТАБЛИЦЯ: Квартири/приміщення
-- Призначення: Приміщення в будівлях з оригінальними ідентифікаторами
-- =================================================================================
CREATE TABLE addrinity.premises (
    id SERIAL PRIMARY KEY,
    building_id INT REFERENCES addrinity.buildings(id), -- Посилання на будівлю
    number TEXT NOT NULL, -- Номер квартири/приміщення
    floor TEXT, -- Поверх
    entrance TEXT, -- Під'їзд
    type TEXT, -- Тип приміщення: 'квартира', 'офіс', 'кімната'
    area DECIMAL(8,2), -- Площа (якщо є)
    -- Оригінальні ідентифікатори
    rtg_premise_id BIGINT, -- ID приміщення з rtg_addr
    ek_addr_premise_key TEXT, -- Ключ приміщення з ek_addr
    bld_local_premise_info JSONB -- Додаткова інформація з bld_local (якщо є)
);

COMMENT ON TABLE addrinity.premises IS 'Квартири/приміщення в будівлях з оригінальними ідентифікаторами';
COMMENT ON COLUMN addrinity.premises.number IS 'Номер квартири/приміщення';
COMMENT ON COLUMN addrinity.premises.floor IS 'Поверх';
COMMENT ON COLUMN addrinity.premises.entrance IS 'Підєзд';
COMMENT ON COLUMN addrinity.premises.type IS 'Тип приміщення: квартира, офіс, кімната';
COMMENT ON COLUMN addrinity.premises.area IS 'Площа приміщення';
COMMENT ON COLUMN addrinity.premises.rtg_premise_id IS 'ID приміщення з системи rtg_addr';
COMMENT ON COLUMN addrinity.premises.ek_addr_premise_key IS 'Ключ приміщення з системи ek_addr';
COMMENT ON COLUMN addrinity.premises.bld_local_premise_info IS 'Додаткова інформація з системи bld_local';

-- Індекси
CREATE INDEX idx_premises_building ON addrinity.premises(building_id);
CREATE INDEX idx_premises_number ON addrinity.premises(number);
CREATE INDEX idx_premises_rtg_id ON addrinity.premises(rtg_premise_id);


-- =================================================================================
-- Призначення: "Рихлий" пошук та ШІ ..........
-- =================================================================================

-- Фонетичний пошук (sounds like)
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Функція нечіткого пошуку вулиць
CREATE OR REPLACE FUNCTION addrinity.fuzzy_street_search(search_term TEXT)
RETURNS TABLE(street_name TEXT, similarity_score REAL) AS $$
BEGIN
    RETURN QUERY
    SELECT name, similarity(name, search_term) as score
    FROM addrinity.street_names
    WHERE similarity(name, search_term) > 0.3
    ORDER BY score DESC
    LIMIT 10;
END;
$$ LANGUAGE plpgsql;


-- Додавання текстового пошуку
ALTER TABLE addrinity.street_names ADD COLUMN search_vector tsvector;
CREATE INDEX idx_street_names_search ON addrinity.street_names USING GIN(search_vector);

-- Тригер для автоматичного оновлення
CREATE OR REPLACE FUNCTION addrinity.update_street_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('ukrainian', NEW.name);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


# Приклад API-endpoint
@app.get("/api/search/address")
async def search_address(query: str, fuzzy: bool = False):
    if fuzzy:
        # Використання нечіткого пошуку
        results = await fuzzy_search_addresses(query)
    else:
        # Точний пошук
        results = await exact_search_addresses(query)
    return results
    
    
    
    
    





