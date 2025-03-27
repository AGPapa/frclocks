CREATE TABLE IF NOT EXISTS following_teams AS (
    WITH
    teams_with_remaining_events AS (
        SELECT
            team_key,
            district_key,
            points,
            rank,
            active_team_rank,
            events_remaining,
            quals_remaining,
            selections_remaining,
            double_elims_remaining,
            finals_remaining,
            awards_remaining,
            points
                + 18 * quals_remaining * 3
                + 16 * selections_remaining * 3
                + 20 * double_elims_remaining * 3
                + 10 * finals_remaining * 3
                + 5 * awards_remaining * 3 -- only 5 points per award, since EI and RAS auto-qualify
            AS max_possible_points
        FROM district_rankings_without_qualify_awards
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
            teams_with_remaining_events.max_possible_points AS following_team_max_possible_points,
            teams_with_remaining_events.max_possible_points >= teams_to_pass.points AS following_team_can_pass,
            CASE WHEN following_team_can_pass THEN
                ROW_NUMBER() OVER (PARTITION BY teams_to_pass.team_key, following_team_can_pass ORDER BY teams_with_remaining_events.rank ASC)
                ELSE NULL
            END AS following_team_order,
            CASE WHEN following_team_can_pass THEN
                CAST(CEIL((teams_to_pass.points - teams_with_remaining_events.points) / 3.0) * 3 AS INTEGER) -- round up to the nearest multiple of 3
            ELSE NULL END AS following_team_points_needed_to_pass,
            CASE WHEN following_team_can_pass THEN CAST(teams_to_pass.points - teams_with_remaining_events.points AS VARCHAR)
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
    ),
    teams_that_can_pass AS (
        SELECT
            team_key,
            SUM(CASE WHEN following_team_can_pass THEN 1 ELSE 0 END) AS total_teams_that_can_pass
        FROM ranked_following_teams
        GROUP BY team_key
    )
    SELECT
        ranked_following_teams.*
    FROM ranked_following_teams
    JOIN max_rank_to_display ON ranked_following_teams.team_key = max_rank_to_display.team_key
    JOIN teams_that_can_pass ON ranked_following_teams.team_key = teams_that_can_pass.team_key
    WHERE ranked_following_teams.following_team_rank <= max_rank_to_display.max_rank OR teams_that_can_pass.total_teams_that_can_pass < ranked_following_teams.teams_to_pass
)