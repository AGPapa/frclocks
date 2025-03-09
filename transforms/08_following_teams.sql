CREATE TABLE IF NOT EXISTS following_teams AS (
    WITH
    teams_with_remaining_events AS (
        SELECT
            district_rankings_without_impact.team_key,
            district_rankings_without_impact.district_key,
            district_rankings_without_impact.points,
            district_rankings_without_impact.inflated_points,
            district_rankings_without_impact.rank
        FROM district_rankings_without_impact
        WHERE events_remaining > 0
    )
    SELECT
        teams_to_pass.team_key,
        teams_to_pass.district_key,
        teams_to_pass.points,
        teams_to_pass.teams_to_pass,
        teams_with_remaining_events.team_key AS following_team_key,
        teams_with_remaining_events.rank AS following_team_rank,
        teams_with_remaining_events.points AS following_team_points,
        teams_with_remaining_events.inflated_points AS following_team_inflated_points,
        ROW_NUMBER() OVER (PARTITION BY teams_to_pass.team_key ORDER BY teams_with_remaining_events.rank ASC) AS following_team_order,
        teams_to_pass.inflated_points - teams_with_remaining_events.inflated_points AS following_team_points_needed_to_pass,
   FROM teams_to_pass
   LEFT JOIN teams_with_remaining_events ON
        teams_to_pass.team_key != teams_with_remaining_events.team_key
        AND teams_with_remaining_events.district_key = teams_to_pass.district_key
   WHERE teams_with_remaining_events.rank >= teams_to_pass.rank
   QUALIFY following_team_order <= teams_to_pass.teams_to_pass
)