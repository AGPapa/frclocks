CREATE TABLE IF NOT EXISTS wcmp_spots_v2 AS (
    WITH alive_alliances AS (
        SELECT DISTINCT
            team_event_states.district_key,
            alliances.alliance_name,
            alliances.event_key,
            alliances.captain_key,
            alliances.first_selection_key,
            alliances.second_selection_key,
            alliances.backup_key
        FROM team_event_states
        JOIN alliances ON team_event_states.event_key = alliances.event_key
            AND team_event_states.team_key IN (alliances.captain_key, alliances.first_selection_key, alliances.second_selection_key, alliances.backup_key)
        JOIN event_states ON team_event_states.event_key = event_states.event_key
        WHERE team_event_states.elim_eligible = TRUE AND event_states.event_state NOT IN ('Pre-Event', 'Qualifications', 'Selections', 'Awards', 'Completed')
    ),
    locked_teams_per_alliance AS (
        SELECT
            alive_alliances.district_key,
            alive_alliances.alliance_name,
            SUM(CASE WHEN lock_status.lock_status IN ('100%', 'Prequalified', 'Impact', 'EI', 'RAS') THEN 1 ELSE 0 END) AS num_locked_teams
        FROM alive_alliances
        LEFT JOIN lock_status ON lock_status.team_key IN (alive_alliances.captain_key, alive_alliances.first_selection_key, alive_alliances.second_selection_key, alive_alliances.backup_key)
            AND lock_status.district_key = alive_alliances.district_key
        GROUP BY 1, 2
    )
    SELECT
        wcmp_spots.district_key,
        ANY_VALUE(wcmp_spots.wcmp_spots)
            + COALESCE(MIN(locked_teams_per_alliance.num_locked_teams), 0) -- guaranteed extra points spots, because no matter which alliance wins, one will be created
        AS wcmp_spots
    FROM wcmp_spots
    LEFT JOIN locked_teams_per_alliance ON wcmp_spots.district_key = locked_teams_per_alliance.district_key
    GROUP BY 1
)