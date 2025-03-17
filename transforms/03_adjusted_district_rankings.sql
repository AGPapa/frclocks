CREATE TABLE IF NOT EXISTS adjusted_district_rankings AS (
    WITH
    events_that_count AS (
        SELECT
            event_teams.team_key,
            events.event_key
        FROM event_teams
        JOIN events ON event_teams.event_key = events.event_key
        WHERE events.event_type = 'District'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_teams.team_key ORDER BY events.start_date ASC) <= 2
    ),
    events_remaining AS (
        SELECT
            events_that_count.team_key,
            event_states.district_key,
            COUNT(*) AS events_remaining,
            SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications') THEN 1 ELSE 0 END) AS quals_remaining,
            SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications', 'Selections') THEN 1 ELSE 0 END) AS selections_remaining,
            SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications', 'Selections', 'Elims 1 to 7') THEN 1 ELSE 0 END) AS double_elims_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Awards', 'Completed') THEN 1 ELSE 0 END) AS finals_remaining,
            SUM(CASE WHEN event_states.event_state NOT IN ('Completed') THEN 1 ELSE 0 END) AS awards_remaining
        FROM events_that_count
        JOIN event_states ON events_that_count.event_key = event_states.event_key
        WHERE event_states.event_state != 'Completed'
        AND event_states.event_type = 'District'
        GROUP BY events_that_count.team_key, event_states.district_key
    ),
    filtered_event_points AS (
        SELECT
            event_states.district_key,
            event_points.team_key,
            SUM(event_points.points) AS points
        FROM event_points
        JOIN event_states ON event_points.event_key = event_states.event_key
        JOIN events_that_count ON event_points.event_key = events_that_count.event_key AND event_points.team_key = events_that_count.team_key
        WHERE event_states.event_type = 'District'
        AND event_states.event_state NOT IN ('Pre-Event', 'Qualifications')
        GROUP BY event_states.district_key, event_points.team_key
    )
    SELECT
        district_rankings.team_key,
        district_rankings.district_key,
        ROW_NUMBER() OVER (PARTITION BY district_rankings.district_key ORDER BY COALESCE(filtered_event_points.points, 0) + district_rankings.rookie_bonus DESC, district_rankings.rank ASC) AS rank,
        COALESCE(filtered_event_points.points, 0) + district_rankings.rookie_bonus AS points,
        district_rankings.rookie_bonus,
        CAST(COALESCE(filtered_event_points.points, 0) + district_rankings.rookie_bonus + 4 * COALESCE(events_remaining.quals_remaining, 0) AS INTEGER) AS inflated_points,
        COALESCE(events_remaining.events_remaining, 0) AS events_remaining,
        COALESCE(events_remaining.quals_remaining, 0) AS quals_remaining,
        COALESCE(events_remaining.selections_remaining, 0) AS selections_remaining,
        COALESCE(events_remaining.double_elims_remaining, 0) AS double_elims_remaining,
        COALESCE(events_remaining.finals_remaining, 0) AS finals_remaining,
        COALESCE(events_remaining.awards_remaining, 0) AS awards_remaining
    FROM district_rankings
    LEFT JOIN filtered_event_points ON district_rankings.team_key = filtered_event_points.team_key AND district_rankings.district_key = filtered_event_points.district_key
    LEFT JOIN events_remaining ON district_rankings.team_key = events_remaining.team_key AND district_rankings.district_key = events_remaining.district_key
)