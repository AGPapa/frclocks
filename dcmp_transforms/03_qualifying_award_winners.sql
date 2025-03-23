CREATE TABLE IF NOT EXISTS qualifying_award_winners AS (
    WITH
    winners AS (
        SELECT
            event_states.district_key,
            matches.winning_alliance,
            ANY_VALUE(alliances.captain_key) AS captain_key,
            ANY_VALUE(alliances.first_selection_key) AS first_selection_key,
            ANY_VALUE(alliances.second_selection_key) AS second_selection_key,
            ANY_VALUE(alliances.backup_key) AS backup_key
        FROM event_states
        JOIN matches ON event_states.event_key = matches.event_key
        LEFT JOIN alliances ON matches.event_key = alliances.event_key
            AND (
                (
                    matches.winning_alliance = 'red'
                    AND matches.red_1_key IN (
                        alliances.captain_key,
                        alliances.first_selection_key,
                        alliances.second_selection_key,
                        alliances.backup_key
                    )
                )
                OR
                (
                    matches.winning_alliance = 'blue'
                    AND matches.blue_1_key IN (
                        alliances.captain_key,
                        alliances.first_selection_key,
                        alliances.second_selection_key,
                        alliances.backup_key
                    )
                )
            )
        WHERE event_states.event_state IN ('Completed', 'Awards')
            AND event_states.event_type = 'District Championship'
            AND matches.comp_level = 'f'
            AND matches.winning_alliance IS NOT NULL
        GROUP BY event_states.district_key, matches.winning_alliance
        HAVING COUNT(*) >= 2
    ),
    winning_teams AS (
        SELECT district_key, captain_key AS team_key FROM winners
        UNION ALL SELECT district_key, first_selection_key AS team_key FROM winners
        UNION ALL SELECT district_key, second_selection_key AS team_key FROM winners
        UNION ALL SELECT district_key, backup_key AS team_key FROM winners WHERE backup_key IS NOT NULL
    ),
    award_teams AS (
        SELECT
            events.district_key,
            awards.team_key,
            CASE WHEN awards.award_type = 0 THEN 'Impact'
            WHEN awards.award_type = 9 THEN 'EI'
            WHEN awards.award_type = 10 THEN 'RAS'
            END AS award_type
        FROM awards
        JOIN events ON awards.event_key = events.event_key
        WHERE award_type IN (0, 9, 10)
        AND events.event_type = 'District Championship'
    )
    SELECT
        COALESCE(winning_teams.district_key, award_teams.district_key, prequalified_teams.district_key) AS district_key,
        COALESCE(winning_teams.team_key, award_teams.team_key, prequalified_teams.team_key) AS team_key,
        award_teams.award_type AS award_type,
        winning_teams.team_key IS NOT NULL AS is_winner,
        prequalified_teams.team_key IS NOT NULL AS is_prequalified
    FROM award_teams
    FULL JOIN winning_teams ON award_teams.district_key = winning_teams.district_key
        AND award_teams.team_key = winning_teams.team_key
    FULL JOIN prequalified_teams ON COALESCE(award_teams.district_key, winning_teams.district_key) = prequalified_teams.district_key
        AND COALESCE(award_teams.team_key, winning_teams.team_key) = prequalified_teams.team_key
)