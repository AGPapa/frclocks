-- TODO: Write comments explaining this
CREATE TABLE IF NOT EXISTS event_points_remaining AS (
    WITH
    qual_points AS (
        SELECT
            event_size,
            CAST(SUM(qual_points) AS INTEGER) AS total_qual_points
        FROM qual_points_lookup
        GROUP BY event_size
    ),
    awarded_award_points AS (
        SELECT
            event_key,
            SUM(award_points) AS awarded_award_points
        FROM event_points
        WHERE award_points != 10 * 3 -- don't count the impact award
        AND award_points != 8 * 3 -- don't count the engineering inspiration award or rookie all star award
        GROUP BY event_key
    ),
    event_teams_count AS (
        SELECT event_key, COUNT(team_key) AS team_count
        FROM event_teams
        GROUP BY event_key
    )
    SELECT
        events.event_key,
        event_teams_count.team_count,
        CASE WHEN event_state != 'Completed' THEN
            60 * 3 -- don't include impact, engineering inspiration, or rookie all star awards
            - COALESCE(awarded_award_points.awarded_award_points, 0)
        ELSE 0 END AS award_points_remaining,
        CASE WHEN event_states.event_state = 'Finals' THEN 0 -- 0 points for match winner since they auto qualify
            WHEN event_states.event_state = 'Elims 13' THEN 0 + 22 -- 21 points for match winner, 1 for possible rounding if there's backup team for 3rd place
            WHEN event_states.event_state = 'Elims 12' THEN 0 + 22 + 19 -- 18 points for match winner, 1 for possible rounding if there's backup team for 4th place
            WHEN event_states.event_state = 'Elims 11' THEN 0 + 22 + 19 + 21 -- 21 points (7 each) for guaranteed 4th place or better
            WHEN event_states.event_state = 'Elims 10' THEN 0 + 22 + 19 + 21 + 21 -- 21 points (7 each) for guaranteed 4th place or better
            WHEN event_states.event_state = 'Elims 9' THEN 0 + 22 + 19 + 21 + 21 + 21 -- 21 points (7 each) for guaranteed 4th place or better
            WHEN event_states.event_state = 'Elims 8' THEN 0 + 22 + 19 + 21 + 21 + 21 + 39 -- 39 points (13 each) for guaranteed 3rd place or better
            WHEN event_states.event_state IN ('Elims 1 to 7', 'Selections', 'Qualifications', 'Pre-Event') THEN 0 + 22 + 19 + 21 + 21 + 21 + 39 + 39
            ELSE 0
        END * 3 AS elimination_points_remaining,
        CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications', 'Selections') THEN 236 ELSE 0 END * 3 AS alliance_selection_points_remaining,
        CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications') THEN qual_points.total_qual_points ELSE 0 END * 3 AS quals_points_remaining,
        quals_points_remaining + alliance_selection_points_remaining + elimination_points_remaining + award_points_remaining AS points_remaining
    FROM events
    JOIN event_states ON events.event_key = event_states.event_key
    JOIN event_teams_count ON events.event_key = event_teams_count.event_key
    JOIN qual_points ON event_teams_count.team_count = qual_points.event_size
    LEFT JOIN awarded_award_points ON events.event_key = awarded_award_points.event_key
)
