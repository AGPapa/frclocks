CREATE TABLE IF NOT EXISTS event_regions AS (
    WITH first_and_second_events AS (
        SELECT
            events.district_key,
            events.event_key,
            event_teams.team_key,
            california_team_lookup.region
        FROM california_team_lookup
        JOIN event_teams ON california_team_lookup.team_key = event_teams.team_key
        JOIN events ON event_teams.event_key = events.event_key
        WHERE events.event_type = 'District'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_teams.team_key ORDER BY events.start_date ASC) <= 2
    )
    SELECT DISTINCT
        district_key,
        event_key,
        region,
        COUNT(*) AS team_count
    FROM first_and_second_events
    GROUP BY district_key, event_key, region
)