CREATE TABLE IF NOT EXISTS impact_award_winners AS (
    SELECT
        events.district_key,
        region_lookup.region,
        awards.team_key
    FROM awards
    JOIN events ON awards.event_key = events.event_key
    JOIN region_lookup ON awards.team_key = region_lookup.team_key
    WHERE award_type = 0
    AND events.event_type = 'District'
)