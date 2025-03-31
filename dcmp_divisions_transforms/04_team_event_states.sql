CREATE TABLE IF NOT EXISTS team_event_states AS (
    WITH eliminated_alliances AS (
        SELECT
            events.district_key,
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
        GROUP BY events.district_key, alliances.alliance_name
        HAVING COUNT(DISTINCT matches.match_key) >= 2
    ),
    eliminated_dcmp_finals_alliances AS (
        SELECT
            events.district_key,
            alliances.alliance_name AS eliminated_alliance_name,
            ANY_VALUE(alliances.captain_key) AS captain_key,
            ANY_VALUE(alliances.first_selection_key) AS first_selection_key,
            ANY_VALUE(alliances.second_selection_key) AS second_selection_key,
            ANY_VALUE(alliances.backup_key) AS backup_key
        FROM events
        JOIN dcmp_finals_state ON dcmp_finals_state.event_key = events.event_key
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
        AND dcmp_finals_state.event_state NOT IN ('Pre-Event', 'Awards', 'Completed')
        GROUP BY events.district_key, alliances.alliance_name
        HAVING COUNT(DISTINCT matches.match_key) >= 2
    ),
    eliminated_teams AS (
        SELECT district_key, captain_key AS team_key FROM eliminated_alliances
        UNION ALL SELECT district_key, first_selection_key AS team_key FROM eliminated_alliances
        UNION ALL SELECT district_key, second_selection_key AS team_key FROM eliminated_alliances
        UNION ALL SELECT district_key, backup_key AS team_key FROM eliminated_alliances WHERE backup_key IS NOT NULL
        UNION ALL SELECT district_key, captain_key AS team_key FROM eliminated_dcmp_finals_alliances
        UNION ALL SELECT district_key, first_selection_key AS team_key FROM eliminated_dcmp_finals_alliances
        UNION ALL SELECT district_key, second_selection_key AS team_key FROM eliminated_dcmp_finals_alliances
        UNION ALL SELECT district_key, backup_key AS team_key FROM eliminated_dcmp_finals_alliances WHERE backup_key IS NOT NULL
    ),
    award_eligible_teams AS (
        SELECT 
            events.district_key,
            event_points.team_key,
            MAX(event_points.award_points) = 0 AS award_eligible
        FROM event_points
        JOIN events ON event_points.event_key = events.event_key
        WHERE events.event_type = 'District Championship Division'
        GROUP BY events.district_key, event_points.team_key
    )
    SELECT
        award_eligible_teams.district_key,
        award_eligible_teams.team_key,
        award_eligible_teams.award_eligible,   -- If a team already earned an award, then they can't win another
        eliminated_teams.team_key IS NULL AS elim_eligible -- If a team has been eliminated in the playoffs, then they can't earn more playoff points. (Not-selcted teams can earn points if they are called as a backup)
    FROM award_eligible_teams
    LEFT JOIN eliminated_teams ON award_eligible_teams.district_key = eliminated_teams.district_key
        AND award_eligible_teams.team_key = eliminated_teams.team_key
)
