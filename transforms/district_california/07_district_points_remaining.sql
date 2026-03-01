CREATE TABLE IF NOT EXISTS district_points_remaining AS (
    SELECT
        event_regions.district_key,
        event_regions.region,
        COUNT(*) AS total_district_events,
        CAST(SUM(event_points_remaining.points_remaining) AS INTEGER) AS points_remaining
    FROM event_regions
    JOIN event_points_remaining ON event_regions.event_key = event_points_remaining.event_key AND event_regions.region = event_points_remaining.region
    GROUP BY event_regions.district_key, event_regions.region
)
