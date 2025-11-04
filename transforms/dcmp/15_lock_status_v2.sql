CREATE TABLE IF NOT EXISTS lock_status_v2 AS (
    SELECT
        adjusted_district_rankings.team_key,
        ANY_VALUE(adjusted_district_rankings.district_key) AS district_key,
        ANY_VALUE(adjusted_district_rankings.rank) AS rank,
        ANY_VALUE(adjusted_district_rankings.district_event_points) AS district_event_points,
        ANY_VALUE(adjusted_district_rankings.dcmp_points) AS dcmp_points,
        ANY_VALUE(adjusted_district_rankings.rookie_bonus) AS rookie_bonus,
        ANY_VALUE(adjusted_district_rankings.points) AS total_points,
        ANY_VALUE(teams_to_pass.teams_to_pass) AS teams_to_pass,
        SUM(CASE WHEN following_teams.following_team_can_pass THEN 1 ELSE 0 END) AS total_teams_that_can_pass,
        CAST(SUM(following_teams.following_team_points_needed_to_pass) AS INTEGER) AS total_points_to_pass,
        ANY_VALUE(district_points_remaining.points_remaining) AS total_points_remaining,
        CASE
        WHEN ANY_VALUE(qualifying_award_winners.is_prequalified) THEN 'Prequalified'
        WHEN ANY_VALUE(qualifying_award_winners.award_type) IS NOT NULL THEN ANY_VALUE(qualifying_award_winners.award_type)
        WHEN ANY_VALUE(qualifying_award_winners.is_winner) THEN 'Winner'
        WHEN ANY_VALUE(district_points_remaining.points_remaining) = 0 AND ANY_VALUE(district_rankings_without_qualify_awards.active_team_rank) <= ANY_VALUE(wcmp_spots.wcmp_spots) THEN '100%'
        WHEN SUM(CASE WHEN following_teams.following_team_can_pass THEN 1 ELSE 0 END) < ANY_VALUE(following_teams.teams_to_pass) THEN '100%'
        WHEN SUM(following_teams.following_team_points_needed_to_pass) > ANY_VALUE(district_points_remaining.points_remaining) THEN '100%'
        WHEN ANY_VALUE(district_rankings_without_qualify_awards.events_remaining) = 0 AND ANY_VALUE(following_teams.following_team_key) IS NULL THEN '-'
        WHEN ANY_VALUE(following_teams.following_team_key) IS NULL THEN '0%'
        ELSE COALESCE(LEAST(ROUND(SUM(following_teams.following_team_points_needed_to_pass) * 100.0 / ANY_VALUE(district_points_remaining.points_remaining), 1), 99.9), 0.0) || '%'
        END AS lock_status,
        CASE
        WHEN lock_status = '100%' THEN '6AA84F'
        WHEN lock_status = '-' THEN 'E06666'
        WHEN lock_status = 'Impact' THEN '6D9EEB'
        WHEN lock_status = 'Winner' THEN '6D9EEB'
        WHEN lock_status = 'EI' THEN '6D9EEB'
        WHEN lock_status = 'RAS' THEN '6D9EEB'
        WHEN lock_status = 'Prequalified' THEN '8E7CC3'
        WHEN ANY_VALUE(following_teams.following_team_key) IS NOT NULL THEN 'B6D7A8'
        ELSE 'FFD966'
        END AS color
    FROM adjusted_district_rankings
    JOIN district_points_remaining ON adjusted_district_rankings.district_key = district_points_remaining.district_key
    JOIN district_lookup ON adjusted_district_rankings.district_key = district_lookup.district_key
    JOIN wcmp_spots_v2 AS wcmp_spots ON adjusted_district_rankings.district_key = wcmp_spots.district_key
    LEFT JOIN teams_to_pass_v2 AS teams_to_pass ON adjusted_district_rankings.team_key = teams_to_pass.team_key
    LEFT JOIN district_rankings_without_qualify_awards ON adjusted_district_rankings.team_key = district_rankings_without_qualify_awards.team_key
    LEFT JOIN following_teams_v2 AS following_teams ON adjusted_district_rankings.team_key = following_teams.team_key
    LEFT JOIN qualifying_award_winners ON
        adjusted_district_rankings.district_key = qualifying_award_winners.district_key
        AND adjusted_district_rankings.team_key = qualifying_award_winners.team_key
    GROUP BY adjusted_district_rankings.team_key
)