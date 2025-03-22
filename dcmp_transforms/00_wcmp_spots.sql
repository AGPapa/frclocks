CREATE TABLE IF NOT EXISTS wcmp_spots AS (
    SELECT
        district_key,
        wcmp_capacity - dcmp_impact_awards - dcmp_ei_awards - dcmp_ras_awards - 4 AS wcmp_spots
    FROM district_lookup
)