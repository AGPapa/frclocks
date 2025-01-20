-- TODO: Write comments explaining this
CREATE TABLE IF NOT EXISTS event_points_remaining AS (
    WITH
    qual_points AS (
        SELECT
            event_size,
            CAST(SUM(qual_points) AS INTEGER) AS total_qual_points,
            total_qual_points - 4 * (event_size) AS total_adjusted_qual_points
        FROM qual_points_lookup
        GROUP BY event_size
    ),
    event_teams_count AS (
        SELECT event_key, COUNT(team_key) AS team_count
        FROM event_teams
        GROUP BY event_key
    ),
    rookie_awards AS (
        SELECT
            event_teams.event_key,
            CASE WHEN COUNT(event_teams.team_key) >= 2 THEN 13
            WHEN COUNT(event_teams.team_key) = 1 THEN 8
            ELSE 0 END AS rookie_award_points
        FROM event_teams
        JOIN district_rankings ON event_teams.team_key = district_rankings.team_key
        WHERE district_rankings.rookie_bonus > 0
        GROUP BY event_teams.event_key
    )
    SELECT
        events.event_key,
        event_teams_count.team_count,
        CASE WHEN event_state != 'Completed' THEN 63 + rookie_awards.rookie_award_points ELSE 0 END AS award_points, -- don't include chairmans or unobtainable rookie awards
        CASE WHEN event_states.event_state = 'Finals' THEN 31 -- 30 for winner, 1 for possible backup team for finalist
            WHEN event_states.event_state = 'Elims 13' THEN 31 + 22 -- 21 for winner, 1 for possible backup team for 3rd place 
            WHEN event_states.event_state = 'Elims 12' THEN 31 + 22 + 19 -- 18 for winner, 1 for possible backup team for 4th place
            WHEN event_states.event_state = 'Elims 11' THEN 31 + 22 + 19 + 21
            WHEN event_states.event_state = 'Elims 10' THEN 31 + 22 + 19 + 21 + 21
            WHEN event_states.event_state = 'Elims 9' THEN 31 + 22 + 19 + 21 + 21 + 21
            WHEN event_states.event_state = 'Elims 8' THEN 31 + 22 + 19 + 21 + 21 + 21 + 39
            WHEN event_states.event_state IN ('Elims 1 to 7', 'Selection', 'Qualifications', 'Pre-Event') THEN 31 + 22 + 19 + 21 + 21 + 21 + 39 + 39
            ELSE 0
        END AS elimination_points,
        CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications', 'Selection') THEN 236 ELSE 0 END AS alliance_selection_points,
        CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications') THEN qual_points.total_adjusted_qual_points ELSE 0 END AS quals_adjusted,
        quals_adjusted + alliance_selection_points + elimination_points + award_points AS points_remaining
    FROM events
    JOIN event_states ON events.event_key = event_states.event_key
    JOIN event_teams_count ON events.event_key = event_teams_count.event_key
    JOIN qual_points ON event_teams_count.team_count = qual_points.event_size
    JOIN rookie_awards ON events.event_key = rookie_awards.event_key
)
