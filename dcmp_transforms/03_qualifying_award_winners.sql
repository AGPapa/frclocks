CREATE TABLE IF NOT EXISTS qualifying_award_winners AS (
    SELECT
        events.district_key,
        awards.team_key,
        awards.award_type
    FROM awards
    JOIN events ON awards.event_key = events.event_key
    WHERE award_type = 0 -- TODO: Add EI and RAS awards
    AND events.event_type = 'District Championship'
)