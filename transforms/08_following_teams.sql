CREATE TABLE IF NOT EXISTS following_teams AS (
    WITH
    teams_with_remaining_events AS (
        SELECT
            team_key,
            district_key,
            points,
            inflated_points,
            rank,
            active_team_rank,
            events_remaining,
            quals_remaining,
            selections_remaining,
            double_elims_remaining,
            finals_remaining,
            awards_remaining,
            inflated_points
                + 18 * quals_remaining
                + 16 * selections_remaining
                + 20 * double_elims_remaining
                + 10 * finals_remaining
                + 8 * awards_remaining
            AS max_possible_points
        FROM district_rankings_without_impact
        WHERE events_remaining > 0
    ),
    ranked_following_teams AS (
        SELECT
            teams_to_pass.team_key,
            teams_to_pass.district_key,
            teams_to_pass.points,
            teams_to_pass.teams_to_pass,
            teams_with_remaining_events.team_key AS following_team_key,
            teams_with_remaining_events.rank AS following_team_rank,
            teams_with_remaining_events.points AS following_team_points,
            teams_with_remaining_events.inflated_points AS following_team_inflated_points,
            teams_with_remaining_events.max_possible_points AS following_team_max_possible_points,
            teams_with_remaining_events.max_possible_points >= teams_to_pass.inflated_points AS following_team_can_pass,
            CASE WHEN following_team_can_pass THEN
                ROW_NUMBER() OVER (PARTITION BY teams_to_pass.team_key, following_team_can_pass ORDER BY teams_with_remaining_events.rank ASC)
                ELSE NULL
            END AS following_team_order,
            CASE WHEN following_team_can_pass THEN
                teams_to_pass.inflated_points - teams_with_remaining_events.inflated_points
            ELSE NULL END AS following_team_points_needed_to_pass,
            CASE WHEN following_team_can_pass THEN CAST(teams_to_pass.inflated_points - teams_with_remaining_events.inflated_points AS VARCHAR)
            ELSE '-' END AS following_team_points_needed_to_pass_status,
            CASE WHEN following_team_can_pass THEN 'FFD966'
            ELSE 'E06666' END AS following_team_color
        FROM teams_to_pass
        LEFT JOIN teams_with_remaining_events ON
                teams_to_pass.team_key != teams_with_remaining_events.team_key
                AND teams_with_remaining_events.district_key = teams_to_pass.district_key
        WHERE teams_with_remaining_events.active_team_rank > teams_to_pass.active_team_rank
    ),
    max_rank_to_display AS (
        SELECT
            team_key,
            MAX(following_team_rank) AS max_rank
        FROM ranked_following_teams
        WHERE following_team_order IS NOT NULL
        AND following_team_order <= teams_to_pass
        GROUP BY team_key
    )
    SELECT
        ranked_following_teams.*
    FROM ranked_following_teams
    JOIN max_rank_to_display ON ranked_following_teams.team_key = max_rank_to_display.team_key
    WHERE ranked_following_teams.following_team_rank <= max_rank_to_display.max_rank
)