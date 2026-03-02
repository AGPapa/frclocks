-- TODO: Write comments explaining this
CREATE TABLE IF NOT EXISTS event_points_remaining AS (
    WITH
    -- Qual points for all events, summed up to each rank, so we can adjust for out-of-region teams
    qual_points AS (
        SELECT
            event_size,
            team_rank AS rank,
            CAST(SUM(qual_points) OVER (PARTITION BY event_size ORDER BY team_rank) AS INTEGER) AS qual_points_through_rank,
            CAST(SUM(qual_points) OVER (PARTITION BY event_size ORDER BY team_rank) - MIN(qual_points) OVER (PARTITION BY event_size) * team_rank AS INTEGER) AS adjusted_qual_points_through_rank
        FROM qual_points_lookup
    ),
    -- Remaining selection points based on the number of teams from this region at this event
    remaining_selection_points AS (
        SELECT
            event_regions.region,
            event_regions.event_key,
            CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications', 'Selections') THEN
                    CASE WHEN event_regions.team_count >= 24 THEN 236
                    WHEN event_regions.team_count = 23 THEN 236 - 1
                    WHEN event_regions.team_count = 22 THEN 236 - 1 - 2
                    WHEN event_regions.team_count = 21 THEN 236 - 1 - 2 - 3
                    WHEN event_regions.team_count = 20 THEN 236 - 1 - 2 - 3 - 4
                    WHEN event_regions.team_count = 19 THEN 236 - 1 - 2 - 3 - 4 - 5
                    WHEN event_regions.team_count = 18 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6
                    WHEN event_regions.team_count = 17 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7
                    WHEN event_regions.team_count = 16 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8
                    WHEN event_regions.team_count = 15 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9
                    WHEN event_regions.team_count = 14 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9
                    WHEN event_regions.team_count = 13 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10
                    WHEN event_regions.team_count = 12 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10
                    WHEN event_regions.team_count = 11 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11
                    WHEN event_regions.team_count = 10 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11
                    WHEN event_regions.team_count = 9 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12
                    WHEN event_regions.team_count = 8 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12
                    WHEN event_regions.team_count = 7 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12 - 13
                    WHEN event_regions.team_count = 6 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12 - 13 - 13
                    WHEN event_regions.team_count = 5 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12 - 13 - 13 - 14
                    WHEN event_regions.team_count = 4 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12 - 13 - 13 - 14 - 14
                    WHEN event_regions.team_count = 3 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12 - 13 - 13 - 14 - 14 - 15
                    WHEN event_regions.team_count = 2 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12 - 13 - 13 - 14 - 14 - 15 - 15
                    WHEN event_regions.team_count = 1 THEN 236 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 9 - 10 - 10 - 11 - 11 - 12 - 12 - 13 - 13 - 14 - 14 - 15 - 15 - 16
                    ELSE 0 END
            ELSE 0 END AS remaining_selection_points
        FROM event_regions
        JOIN event_states ON event_regions.event_key = event_states.event_key
    ),
    -- Remaining eliminiations points are capped by the number of region teams at the event
    -- TODO: Can minimize this further by looking for the first two events
    remaining_elimination_points AS (
        SELECT
            california_team_lookup.region,
            event_teams.event_key,
            SUM(CASE WHEN COALESCE(team_event_states.elim_eligible, TRUE) THEN 1 ELSE 0 END) AS elim_teams_remaining,
            CASE WHEN elim_teams_remaining = 0 THEN 0
            WHEN elim_teams_remaining <= 3 THEN 30 * elim_teams_remaining
            WHEN elim_teams_remaining <= 6 THEN 30 * 3 + 20 * (elim_teams_remaining - 3)
            WHEN elim_teams_remaining <= 9 THEN 30 * 3 + 20 * 3 + 13 * (elim_teams_remaining - 6)
            WHEN elim_teams_remaining <= 12 THEN 30 * 3 + 20 * 3 + 7 * (elim_teams_remaining - 9)
            ELSE 213 END AS max_elimination_points
        FROM california_team_lookup
        JOIN event_teams ON california_team_lookup.team_key = event_teams.team_key
        JOIN event_states ON event_teams.event_key = event_states.event_key
        LEFT JOIN team_event_states ON event_states.event_key = team_event_states.event_key AND event_teams.team_key = team_event_states.team_key
        GROUP BY california_team_lookup.region, event_teams.event_key
    ),
    awarded_award_points AS (
        SELECT
            event_key,
            SUM(award_points) AS awarded_award_points
        FROM event_points
        WHERE award_points != 10 -- don't count the impact award
        GROUP BY event_key
    ),
    event_teams_count AS (
        SELECT event_key, COUNT(team_key) AS team_count
        FROM event_teams
        GROUP BY event_key
    ),
    rookie_awards AS (
        SELECT
            event_teams.event_key,
            CASE WHEN COUNT(event_teams.team_key) >= 1 THEN 8
            ELSE 0 END AS rookie_award_points
        FROM event_teams
        JOIN district_rankings ON event_teams.team_key = district_rankings.team_key
        WHERE district_rankings.rookie_bonus = 10
        GROUP BY event_teams.event_key
    )
    SELECT
        event_regions.region,
        events.event_key,
        event_regions.team_count,
        CASE WHEN event_state != 'Completed' THEN
            68 -- don't include impact or unobtainable rookie awards
            + COALESCE(rookie_awards.rookie_award_points, 0)
            - COALESCE(awarded_award_points.awarded_award_points, 0)
        ELSE 0 END AS award_points_remaining,
        LEAST(
            remaining_elimination_points.max_elimination_points,
            CASE WHEN event_states.event_state = 'Finals' THEN 31 -- 30 points for match winner, 1 for possible rounding if there's backup team for finalist
                WHEN event_states.event_state = 'Elims 13' THEN 31 + 22 -- 21 points for match winner, 1 for possible rounding if there's backup team for 3rd place
                WHEN event_states.event_state = 'Elims 12' THEN 31 + 22 + 19 -- 18 points for match winner, 1 for possible rounding if there's backup team for 4th place
                WHEN event_states.event_state = 'Elims 11' THEN 31 + 22 + 19 + 21 -- 21 points (7 each) for guaranteed 4th place or better
                WHEN event_states.event_state = 'Elims 10' THEN 31 + 22 + 19 + 21 + 21 -- 21 points (7 each) for guaranteed 4th place or better
                WHEN event_states.event_state = 'Elims 9' THEN 31 + 22 + 19 + 21 + 21 + 21 -- 21 points (7 each) for guaranteed 4th place or better
                WHEN event_states.event_state = 'Elims 8' THEN 31 + 22 + 19 + 21 + 21 + 21 + 39 -- 39 points (13 each) for guaranteed 3rd place or better
                WHEN event_states.event_state IN ('Elims 1 to 7', 'Selections', 'Qualifications', 'Pre-Event') THEN 31 + 22 + 19 + 21 + 21 + 21 + 39 + 39
                ELSE 0
            END
        ) AS elimination_points_remaining,
        remaining_selection_points.remaining_selection_points AS alliance_selection_points_remaining,
        CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualifications') THEN qual_points.adjusted_qual_points_through_rank ELSE 0 END AS quals_adjusted_points_remaining,
        quals_adjusted_points_remaining + alliance_selection_points_remaining + elimination_points_remaining + award_points_remaining AS points_remaining
    FROM events
    JOIN event_regions ON events.event_key = event_regions.event_key
    JOIN event_states ON events.event_key = event_states.event_key
    JOIN event_teams_count ON events.event_key = event_teams_count.event_key
    JOIN qual_points ON event_teams_count.team_count = qual_points.event_size AND event_regions.team_count = qual_points.rank
    JOIN remaining_selection_points ON event_regions.region = remaining_selection_points.region AND events.event_key = remaining_selection_points.event_key
    JOIN remaining_elimination_points ON event_regions.region = remaining_elimination_points.region AND events.event_key = remaining_elimination_points.event_key
    LEFT JOIN rookie_awards ON events.event_key = rookie_awards.event_key
    LEFT JOIN awarded_award_points ON events.event_key = awarded_award_points.event_key
)
