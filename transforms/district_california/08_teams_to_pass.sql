CREATE TABLE IF NOT EXISTS teams_to_pass AS (
    WITH num_events AS (
        SELECT
            event_regions.district_key,
            event_regions.region,
            COUNT(*) AS num_events
        FROM event_regions
        GROUP BY event_regions.district_key, event_regions.region
    )
    SELECT
        district_rankings_without_impact.district_key,
        district_rankings_without_impact.region,
        district_rankings_without_impact.team_key,
        district_rankings_without_impact.points,
        district_rankings_without_impact.inflated_points,
        district_rankings_without_impact.rank,
        district_rankings_without_impact.active_team_rank,
        (district_lookup.dcmp_capacity - num_events.num_events) - district_rankings_without_impact.active_team_rank + 1 AS teams_to_pass
    FROM district_rankings_without_impact
    JOIN district_lookup ON district_rankings_without_impact.district_key = district_lookup.district_key
    JOIN num_events ON district_rankings_without_impact.district_key = num_events.district_key AND district_rankings_without_impact.region = num_events.region
    WHERE (district_lookup.dcmp_capacity - num_events.num_events) - district_rankings_without_impact.active_team_rank + 1 > 0
)