CREATE TABLE IF NOT EXISTS adjusted_district_rankings AS (
    WITH
    district_events AS (
        SELECT
            event_points.team_key,
            events.event_key,
            event_points.points
        FROM event_points
        JOIN events ON event_points.event_key = events.event_key
        WHERE events.event_type = 'District'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_points.team_key ORDER BY events.start_date ASC) <= 2
    ),
    district_event_points AS (
        SELECT
            district_events.team_key,
            SUM(district_events.points) AS points
        FROM district_events
        GROUP BY district_events.team_key
    ),
    dcmp AS (
        SELECT
            event_points.team_key,
            event_states.district_key,
            SUM(event_points.points) AS points,
            SUM(CASE WHEN event_states.event_state NOT IN ('Completed') THEN 1 ELSE 0 END) AS events_remaining,
            SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications') THEN 1 ELSE 0 END) AS quals_remaining,
            SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications', 'Selections') THEN 1 ELSE 0 END) AS selections_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Finals', 'Awards', 'Completed')  AND COALESCE(team_event_states.elim_eligible, TRUE) THEN 1 ELSE 0 END) AS double_elims_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Awards', 'Completed') AND COALESCE(team_event_states.elim_eligible, TRUE) THEN 1 ELSE 0 END) AS finals_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Completed') AND COALESCE(team_event_states.award_eligible, TRUE) THEN 1 ELSE 0 END) AS awards_remaining
        FROM event_points
        JOIN event_states ON event_points.event_key = event_states.event_key
        LEFT JOIN team_event_states ON event_points.event_key = event_states.event_key
                            AND team_event_states.team_key = event_points.team_key
        WHERE event_states.event_state != 'Completed'
        AND event_states.event_type = 'District Championship'
        GROUP BY event_points.team_key, event_states.district_key
    )
    SELECT
        district_rankings.team_key,
        district_rankings.district_key,
        ROW_NUMBER() OVER (PARTITION BY district_rankings.district_key ORDER BY COALESCE(district_event_points.points, 0) + district_rankings.rookie_bonus + COALESCE(dcmp.points, 0) DESC, district_rankings.rank ASC) AS rank,
        district_event_points.points AS district_event_points,
        district_rankings.rookie_bonus,
        dcmp.points AS dcmp_points,
        COALESCE(district_event_points.points, 0) + district_rankings.rookie_bonus + COALESCE(dcmp.points, 0) AS points,
        COALESCE(dcmp.events_remaining, 0) AS events_remaining,
        COALESCE(dcmp.quals_remaining, 0) AS quals_remaining,
        COALESCE(dcmp.selections_remaining, 0) AS selections_remaining,
        COALESCE(dcmp.double_elims_remaining, 0) AS double_elims_remaining,
        COALESCE(dcmp.finals_remaining, 0) AS finals_remaining,
        COALESCE(dcmp.awards_remaining, 0) AS awards_remaining
    FROM district_rankings
    LEFT JOIN dcmp ON district_rankings.team_key = dcmp.team_key
    LEFT JOIN district_event_points ON district_rankings.team_key = district_event_points.team_key
    -- TODO: Add points adjustments
)
