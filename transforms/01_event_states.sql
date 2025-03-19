CREATE TABLE IF NOT EXISTS event_states AS (
    WITH award_state AS (
        SELECT
            events.event_key,
            ANY_VALUE(events.name) AS name,
            COUNT(DISTINCT awards.award_type) AS award_count
        FROM events
        LEFT JOIN awards ON events.event_key = awards.event_key
        GROUP BY events.event_key
    ),
    alliance_state AS (
        SELECT
            events.event_key,
            COUNT(DISTINCT alliances.alliance_name) AS alliance_count
        FROM events
        LEFT JOIN alliances ON events.event_key = alliances.event_key
        GROUP BY events.event_key
    ),
    quals_state AS (
        SELECT
            events.event_key,
            COUNT(DISTINCT matches.match_key) AS match_count,
            COUNT(COALESCE(NULLIF(matches.red_score, -1), NULLIF(matches.blue_score, -1))) AS completed_match_count
        FROM events
        LEFT JOIN matches ON events.event_key = matches.event_key
        WHERE matches.comp_level = 'qm'
        GROUP BY events.event_key
    )
    SELECT
        events.event_key,
        ANY_VALUE(events.district_key) AS district_key,
        ANY_VALUE(events.name) AS name,
        ANY_VALUE(events.event_type) AS event_type,
        ANY_VALUE(events.start_date) AS start_date,
        ANY_VALUE(events.end_date) AS end_date,
        CASE
            WHEN COALESCE(ANY_VALUE(quals_state.match_count), 0) = 0 THEN 'Pre-Event'
            WHEN ANY_VALUE(quals_state.completed_match_count) < ANY_VALUE(quals_state.match_count) THEN 'Qualifications'
            WHEN ANY_VALUE(quals_state.completed_match_count) = ANY_VALUE(quals_state.match_count) AND ANY_VALUE(alliance_state.alliance_count) = 0 THEN 'Selections'
            WHEN ANY_VALUE(alliance_state.alliance_count) > 0 AND SUM(CASE WHEN matches.comp_level = 'sf' AND matches.winning_alliance IN ('red', 'blue') THEN 1 ELSE 0 END) < 7 THEN 'Elims 1 to 7'
            WHEN SUM(CASE WHEN matches.comp_level = 'sf' AND matches.winning_alliance IN ('red', 'blue') THEN 1 ELSE 0 END) BETWEEN 1 AND 12 THEN 'Elims ' || (SUM(CASE WHEN matches.comp_level = 'sf' AND matches.winning_alliance IN ('red', 'blue') THEN 1 ELSE 0 END) + 1)
            WHEN SUM(CASE WHEN matches.comp_level = 'f' AND matches.winning_alliance = 'red' THEN 1 ELSE 0 END) < 2 AND SUM(CASE WHEN matches.comp_level = 'f' AND matches.winning_alliance = 'blue' THEN 1 ELSE 0 END) < 2 THEN 'Finals'
            WHEN ANY_VALUE(award_state.award_count) <= 10 THEN 'Awards'
            WHEN ANY_VALUE(award_state.award_count) > 10 THEN 'Completed'
            ELSE 'ERROR'
        END AS event_state,
        CASE WHEN event_state = 'Completed' THEN '93C47D'
        ELSE 'FFD966'
        END AS color
    FROM events
    LEFT JOIN award_state ON events.event_key = award_state.event_key
    LEFT JOIN alliance_state ON events.event_key = alliance_state.event_key
    LEFT JOIN quals_state ON events.event_key = quals_state.event_key
    LEFT JOIN matches ON events.event_key = matches.event_key
    WHERE events.event_type = 'District'
    GROUP BY
        events.event_key
)
