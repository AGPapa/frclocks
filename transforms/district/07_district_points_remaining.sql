CREATE TABLE IF NOT EXISTS district_points_remaining AS (
    SELECT
        events.district_key,
        COUNT(*) AS total_district_events,
        SUM(CASE WHEN event_states.event_state != 'Completed' THEN 1 ELSE 0 END) AS events_remaining,
        CAST(SUM(event_points_remaining.points_remaining) AS INTEGER) AS points_remaining
    FROM events
    JOIN event_states ON events.event_key = event_states.event_key
    JOIN event_points_remaining ON events.event_key = event_points_remaining.event_key
    WHERE events.event_type = 'District'
    GROUP BY events.district_key
)
