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
            event_teams.team_key,
            event_states.district_key,
            event_states.event_key,
            SUM(CASE WHEN event_states.event_state NOT IN ('Pre-Event', 'Qualifications') THEN event_points.points ELSE 0 END) AS points,
            SUM(CASE WHEN dcmp_finals_state.event_state NOT IN ('Completed') THEN 1 ELSE 0 END) AS events_remaining,
            SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications') THEN 1 ELSE 0 END) AS quals_remaining,
            SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications', 'Selections') THEN 1 ELSE 0 END) AS selections_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Finals', 'Awards', 'Completed')  AND COALESCE(team_event_states.elim_eligible, TRUE) THEN 1 ELSE 0 END) AS double_elims_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Awards', 'Completed') AND COALESCE(team_event_states.elim_eligible, TRUE) THEN 1 ELSE 0 END) AS finals_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Completed') AND COALESCE(team_event_states.award_eligible, TRUE) THEN 1 ELSE 0 END) AS awards_remaining,
            SUM(CASE WHEN dcmp_finals_state.num_dcmp_divisions = 4 AND dcmp_finals_state.event_state NOT IN ('Finals', 'Awards', 'Completed') AND COALESCE(team_event_states.elim_eligible, TRUE) THEN 1 ELSE 0 END) AS dcmp_semi_finals_remaining
        FROM event_teams
        JOIN event_states ON event_teams.event_key = event_states.event_key
        JOIN dcmp_finals_state ON event_states.district_key = dcmp_finals_state.district_key
        LEFT JOIN event_points ON event_teams.event_key = event_points.event_key AND event_points.team_key = event_teams.team_key
        LEFT JOIN team_event_states ON team_event_states.district_key = event_states.district_key
            AND team_event_states.team_key = event_teams.team_key
        WHERE event_states.event_type = 'District Championship Division'
        GROUP BY event_teams.team_key, event_states.district_key, event_states.event_key
    ),
    dcmp_finals AS (
        SELECT
            dcmp_finals_state.district_key,
            event_teams.team_key,
            SUM(event_points.points) AS points
        FROM event_teams
        JOIN dcmp_finals_state ON event_teams.event_key = dcmp_finals_state.event_key
        JOIN event_points ON event_teams.event_key = event_points.event_key AND event_points.team_key = event_teams.team_key
        WHERE dcmp_finals_state.event_type = 'District Championship'
        GROUP BY dcmp_finals_state.district_key, event_teams.team_key
    )
    SELECT
        district_rankings.team_key,
        district_rankings.district_key,
        ROW_NUMBER() OVER (PARTITION BY district_rankings.district_key ORDER BY COALESCE(district_event_points.points, 0) + district_rankings.rookie_bonus + COALESCE(points_adjustments.points_adjustment, 0) + COALESCE(dcmp.points, 0) + COALESCE(dcmp_finals.points, 0) DESC, district_rankings.rank ASC) AS rank,
        district_event_points.points AS district_event_points,
        district_rankings.rookie_bonus + COALESCE(points_adjustments.points_adjustment, 0) AS rookie_bonus,
        COALESCE(dcmp.points, 0) + COALESCE(dcmp_finals.points, 0) AS dcmp_points,
        COALESCE(district_event_points.points, 0) + district_rankings.rookie_bonus + COALESCE(points_adjustments.points_adjustment, 0) + COALESCE(dcmp.points, 0) + COALESCE(dcmp_finals.points, 0)AS points,
        COALESCE(dcmp.events_remaining, 0) AS events_remaining,
        COALESCE(dcmp.quals_remaining, 0) AS quals_remaining,
        COALESCE(dcmp.selections_remaining, 0) AS selections_remaining,
        COALESCE(dcmp.double_elims_remaining, 0) AS double_elims_remaining,
        COALESCE(dcmp.finals_remaining, 0) AS finals_remaining,
        COALESCE(dcmp.awards_remaining, 0) AS awards_remaining,
        COALESCE(dcmp.dcmp_semi_finals_remaining, 0) AS dcmp_semi_finals_remaining
    FROM district_rankings
    LEFT JOIN dcmp ON district_rankings.team_key = dcmp.team_key
    LEFT JOIN district_event_points ON district_rankings.team_key = district_event_points.team_key
    LEFT JOIN dcmp_finals ON district_rankings.team_key = dcmp_finals.team_key
    LEFT JOIN points_adjustments ON district_rankings.team_key = points_adjustments.team_key AND district_rankings.district_key = points_adjustments.district_key
)
