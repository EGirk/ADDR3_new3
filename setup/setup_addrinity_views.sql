-- ================================
-- VIEW: Повна адреса з деталями
-- ================================
CREATE OR REPLACE VIEW addrinity.v_full_address AS
SELECT
    cntry.name_uk AS country,
    rgn.name_uk AS region,
    dst.name_uk AS district,
    cm.name_uk AS community,
    ct.name_uk AS city,
    ctd.name_uk AS city_district,
    stn.name AS street,
    stn.language_code,
    bld.number AS building_number,
    prm.number AS premise_number,
    prm.type AS premise_type,
    bld.postal_code,
    bld.latitude,
    bld.longitude
FROM addrinity.premises prm
LEFT JOIN addrinity.buildings bld ON prm.building_id = bld.id
LEFT JOIN addrinity.street_entities ste ON bld.street_entity_id = ste.id
LEFT JOIN addrinity.street_names stn ON stn.street_entity_id = ste.id AND stn.is_current = TRUE AND stn.is_official = TRUE
LEFT JOIN addrinity.street_types stt ON ste.type_id = stt.id
LEFT JOIN addrinity.city_districts ctd ON ste.city_district_id = ctd.id
LEFT JOIN addrinity.cities ct ON ste.city_id = ct.id
LEFT JOIN addrinity.communities cm ON ct.community_id = cm.id
LEFT JOIN addrinity.districts dst ON cm.district_id = dst.id
LEFT JOIN addrinity.regions rgn ON dst.region_id = rgn.id
LEFT JOIN addrinity.countries cntry ON rgn.country_id = cntry.id;

-- ================================
-- VIEW: Облік житлового фонду
-- ================================
CREATE OR REPLACE VIEW addrinity.v_housing_stats AS
SELECT
    rgn.name_uk AS region,
    dst.name_uk AS district,
    ct.name_uk AS city,
    COUNT(bld.id) AS total_buildings,
    SUM(bld.apartments) AS total_apartments,
    SUM(bld.entrances) AS total_entrances
FROM addrinity.buildings bld
LEFT JOIN addrinity.street_entities ste ON bld.street_entity_id = ste.id
LEFT JOIN addrinity.cities ct ON ste.city_id = ct.id
LEFT JOIN addrinity.communities cm ON ct.community_id = cm.id
LEFT JOIN addrinity.districts dst ON cm.district_id = dst.id
LEFT JOIN addrinity.regions rgn ON dst.region_id = rgn.id
GROUP BY rgn.name_uk, dst.name_uk, ct.name_uk;

-- ================================
-- VIEW: Історія назв вулиць
-- ================================
CREATE OR REPLACE VIEW addrinity.v_street_name_history AS
SELECT
    sname.street_entity_id,
    sname.name,
    sname.language_code,
    sname.name_type,
    sname.valid_from,
    sname.valid_to,
    sname.is_official,
    sname.is_current
FROM addrinity.street_names sname
ORDER BY sname.street_entity_id, sname.valid_from NULLS FIRST;

-- ================================
-- VIEW: Геокоординати будівель
-- ================================
CREATE OR REPLACE VIEW addrinity.v_building_geo AS
SELECT
    bld.id AS building_id,
    ct.name_uk AS city,
    stn.name AS street,
    bld.number,
    bld.latitude,
    bld.longitude
FROM addrinity.buildings bld
LEFT JOIN addrinity.street_entities ste ON bld.street_entity_id = ste.id
LEFT JOIN addrinity.street_names stn ON stn.street_entity_id = ste.id AND stn.is_current = TRUE AND stn.is_official = TRUE
LEFT JOIN addrinity.cities ct ON ste.city_id = ct.id
WHERE bld.latitude IS NOT NULL AND bld.longitude IS NOT NULL;