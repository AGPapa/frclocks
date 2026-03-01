CREATE TABLE IF NOT EXISTS team_event_states AS (
    WITH eliminated_alliances AS (
        SELECT
            events.event_key,
            alliances.alliance_name AS eliminated_alliance_name,
            ANY_VALUE(alliances.captain_key) AS captain_key,
            ANY_VALUE(alliances.first_selection_key) AS first_selection_key,
            ANY_VALUE(alliances.second_selection_key) AS second_selection_key,
            ANY_VALUE(alliances.backup_key) AS backup_key
        FROM events
        JOIN event_states ON event_states.event_key = events.event_key
        LEFT JOIN matches ON events.event_key = matches.event_key
        LEFT JOIN alliances ON events.event_key = alliances.event_key
            AND (
                (
                    matches.winning_alliance = 'blue'
                    AND matches.red_1_key IN (
                        alliances.captain_key,
                        alliances.first_selection_key,
                        alliances.second_selection_key,
                        alliances.backup_key
                    )
                )
                OR
                (
                    matches.winning_alliance = 'red'
                    AND matches.blue_1_key IN (
                        alliances.captain_key,
                        alliances.first_selection_key,
                        alliances.second_selection_key,
                        alliances.backup_key
                    )
                )
            )
        WHERE matches.comp_level = 'sf'
        AND winning_alliance IN ('red', 'blue')
        AND event_states.event_state NOT IN ('Pre-Event', 'Qualifications', 'Selections', 'Awards', 'Completed')
        GROUP BY events.event_key, alliances.alliance_name
        HAVING COUNT(DISTINCT matches.match_key) >= 2
    ),
    eliminated_teams AS (
        SELECT event_key, captain_key AS team_key FROM eliminated_alliances
        UNION ALL SELECT event_key, first_selection_key AS team_key FROM eliminated_alliances
        UNION ALL SELECT event_key, second_selection_key AS team_key FROM eliminated_alliances
        UNION ALL SELECT event_key, backup_key AS team_key FROM eliminated_alliances WHERE backup_key IS NOT NULL
    )
    SELECT
        event_states.event_key,
        event_points.team_key,
        event_points.award_points = 0 AS award_eligible,   -- If a team already earned an award, then they can't win another
        eliminated_teams.team_key IS NULL AS elim_eligible -- If a team has been eliminated in the playoffs, then they can't earn more playoff points. (Not-selcted teams can earn points if they are called as a backup)
    FROM event_points
    JOIN event_states ON event_points.event_key = event_states.event_key
    LEFT JOIN eliminated_teams ON event_points.event_key = eliminated_teams.event_key
                            AND event_points.team_key = eliminated_teams.team_key
    WHERE event_states.event_state NOT IN ('Pre-Event', 'Qualifications', 'Selections', 'Completed')
)