CREATE TABLE IF NOT EXISTS dcmp_finals_state AS (
    -- Pre-Event, Elims 1, Elims 2, Elims 3, Elims 4, Elims 5, Finals, Awards, Completed
    with event_awards AS (
        SELECT
            district_key,
            COUNT(*) AS impact_count
        FROM qualifying_award_winners
        WHERE award_type = 'Impact'
        GROUP BY district_key
    ),
    num_dcmp_divisions AS (
        SELECT
            district_key,
            COUNT(DISTINCT event_key) AS num_dcmp_divisions
        FROM event_states
        WHERE event_type = 'District Championship Division'
        GROUP BY district_key
    )
    SELECT
        events.district_key,
        events.event_key,
        ANY_VALUE(district_lookup.display_name || ' ' || district_lookup.dcmp_name || ' Finals') AS name,
        ANY_VALUE(events.event_type) AS event_type,
        ANY_VALUE(num_dcmp_divisions.num_dcmp_divisions) AS num_dcmp_divisions,
        CASE
            WHEN COALESCE(COUNT(DISTINCT matches.match_key), 0) = 0 THEN 'Pre-Event'
            WHEN SUM(CASE WHEN matches.comp_level IN ('sf', 'f') AND matches.winning_alliance IN ('red', 'blue') THEN 1 ELSE 0 END) = 0 THEN 'Elims 1'
            WHEN SUM(CASE WHEN matches.comp_level = 'sf' AND matches.winning_alliance IN ('red', 'blue') THEN 1 ELSE 0 END) BETWEEN 1 AND 4 THEN 'Elims ' || (SUM(CASE WHEN matches.comp_level = 'sf' AND matches.winning_alliance IN ('red', 'blue') THEN 1 ELSE 0 END) + 1)
            WHEN SUM(CASE WHEN matches.comp_level = 'f' AND matches.winning_alliance = 'red' THEN 1 ELSE 0 END) < 2 AND SUM(CASE WHEN matches.comp_level = 'f' AND matches.winning_alliance = 'blue' THEN 1 ELSE 0 END) < 2 THEN 'Finals'
            WHEN COALESCE(ANY_VALUE(event_awards.impact_count), 0) < ANY_VALUE(district_lookup.dcmp_impact_awards) THEN 'Awards'
            WHEN COALESCE(ANY_VALUE(event_awards.impact_count), 0) >= ANY_VALUE(district_lookup.dcmp_impact_awards) THEN 'Completed'
            ELSE 'ERROR'
        END AS event_state,
        CASE WHEN event_state = 'Completed' THEN 'ECFDF5'
        ELSE 'fefce8'
        END AS color
    FROM events
    JOIN district_lookup ON events.district_key = district_lookup.district_key
    JOIN num_dcmp_divisions ON events.district_key = num_dcmp_divisions.district_key
    LEFT JOIN matches ON events.event_key = matches.event_key
    LEFT JOIN event_awards ON events.district_key = event_awards.district_key
    WHERE events.event_type = 'District Championship'
    GROUP BY events.district_key, events.event_key
)
