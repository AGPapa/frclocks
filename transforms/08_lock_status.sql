CREATE TABLE IF NOT EXISTS lock_status AS (
    WITH points_by_event AS (
        SELECT
            event_points.event_key,
            event_points.team_key,
            event_points.points AS points,
            ROW_NUMBER() OVER (PARTITION BY event_points.team_key ORDER BY events.start_date) AS event_number
        FROM event_points
        JOIN events ON event_points.event_key = events.event_key
        JOIN district_rankings ON event_points.team_key = district_rankings.team_key
        WHERE events.event_type = 'District' and district_rankings.district_key = events.district_key
    )
    SELECT
        district_rankings.team_key,
        ANY_VALUE(district_rankings.district_key) AS district_key,
        ANY_VALUE(district_rankings.rank) AS rank,
        ANY_VALUE(district_rankings.points) AS total_points,
        ANY_VALUE(district_rankings_without_impact.inflated_points) AS total_inflated_points,
        COALESCE(ANY_VALUE(event_points_1.points), 0) AS event1_points,
        COALESCE(ANY_VALUE(event_points_2.points), 0) AS event2_points,
        ANY_VALUE(district_rankings.rookie_bonus) AS rookie_bonus,
        CAST(SUM(following_teams.following_team_points_needed_to_pass) AS INTEGER) AS total_points_to_pass,
        ANY_VALUE(district_points_remaining.points_remaining) AS total_points_remaining,
        CASE
        WHEN ANY_VALUE(impact_award_winners.team_key) IS NOT NULL THEN 'Impact'
        WHEN COUNT(following_teams.team_key) < ANY_VALUE(following_teams.teams_to_pass) THEN '100%'
        WHEN SUM(following_teams.following_team_points_needed_to_pass) > ANY_VALUE(district_points_remaining.points_remaining) THEN '100%'
        WHEN ANY_VALUE(district_rankings_without_impact.events_remaining) = 0 AND ANY_VALUE(following_teams.following_team_key) IS NULL THEN '-'
        WHEN ANY_VALUE(following_teams.following_team_key) IS NULL THEN '0%'
        ELSE COALESCE(FLOOR(SUM(following_teams.following_team_points_needed_to_pass) * 100.0 / ANY_VALUE(district_points_remaining.points_remaining)), 0) || '%'
        END AS lock_status,
        CASE
        WHEN lock_status = '100%' THEN '6AA84F'
        WHEN lock_status = '-' THEN 'E06666'
        WHEN lock_status = 'Impact' THEN '6D9EEB'
        ELSE 'FFD966'
        END AS color
    FROM district_rankings
    JOIN district_points_remaining ON district_rankings.district_key = district_points_remaining.district_key
    JOIN district_lookup ON district_rankings.district_key = district_lookup.district_key
    LEFT JOIN district_rankings_without_impact ON district_rankings.team_key = district_rankings_without_impact.team_key
    LEFT JOIN following_teams ON district_rankings.team_key = following_teams.team_key
    LEFT JOIN impact_award_winners ON district_rankings.team_key = impact_award_winners.team_key
    LEFT JOIN points_by_event AS event_points_1 ON district_rankings.team_key = event_points_1.team_key AND event_points_1.event_number = 1
    LEFT JOIN points_by_event AS event_points_2 ON district_rankings.team_key = event_points_2.team_key AND event_points_2.event_number = 2
    GROUP BY district_rankings.team_key
)