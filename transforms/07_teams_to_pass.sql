CREATE TABLE IF NOT EXISTS teams_to_pass AS (
    WITH num_events_remaining AS (
        SELECT
            event_states.district_key,
            COUNT(*) AS num_events
        FROM event_states
        WHERE event_states.event_state != 'Completed'
        GROUP BY event_states.district_key
    )
    SELECT
        district_rankings_without_impact.district_key,
        district_rankings_without_impact.team_key,
        district_rankings_without_impact.points,
        district_rankings_without_impact.inflated_points,
        district_rankings_without_impact.rank,
        district_rankings_without_impact.active_team_rank,
        (district_lookup.dcmp_capacity - num_events_remaining.num_events) - district_rankings_without_impact.active_team_rank + 1 AS teams_to_pass
    FROM district_rankings_without_impact
    JOIN district_lookup ON district_rankings_without_impact.district_key = district_lookup.district_key
    JOIN num_events_remaining ON district_rankings_without_impact.district_key = num_events_remaining.district_key
    WHERE (district_lookup.dcmp_capacity - num_events_remaining.num_events) - district_rankings_without_impact.active_team_rank + 1 > 0
)