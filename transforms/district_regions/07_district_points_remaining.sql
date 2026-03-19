CREATE TABLE IF NOT EXISTS district_points_remaining AS (
    SELECT
        event_regions.district_key,
        event_regions.region,
        COUNT(*) AS total_district_events,
        SUM(CASE WHEN event_states.event_state != 'Completed' THEN 1 ELSE 0 END) AS events_remaining,
        CAST(SUM(event_points_remaining.points_remaining) AS INTEGER) AS points_remaining
    FROM event_regions
    JOIN event_states ON event_regions.event_key = event_states.event_key
    JOIN event_points_remaining ON event_regions.event_key = event_points_remaining.event_key AND event_regions.region = event_points_remaining.region
    WHERE event_states.event_type = 'District'
    GROUP BY event_regions.district_key, event_regions.region
)
