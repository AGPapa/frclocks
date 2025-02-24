CREATE TABLE IF NOT EXISTS district_rankings_without_impact AS (
    WITH events_remaining AS (
        SELECT
            event_teams.team_key,
            event_states.district_key,
            LEAST(COUNT(*), 2) AS events_remaining,
            LEAST(SUM(CASE WHEN event_states.event_state IN ('Pre-Event', 'Qualification') THEN 1 ELSE 0 END), 2) AS incomplete_quals
        FROM event_teams
        JOIN event_states ON event_teams.event_key = event_states.event_key
        WHERE event_states.event_state != 'Completed'
        AND event_states.event_type = 'District'
        GROUP BY event_teams.team_key, event_states.district_key
    )
    SELECT
        district_rankings.team_key,
        district_rankings.district_key,
        district_rankings.rank,
        district_rankings.points,
        district_rankings.rookie_bonus,
        CAST(district_rankings.points + 4 * coalesce(events_remaining.incomplete_quals, 0) AS INTEGER) AS inflated_points,
        ROW_NUMBER() OVER (PARTITION BY district_rankings.district_key ORDER BY inflated_points DESC, district_rankings.rank ASC) AS active_team_rank,
        COALESCE(events_remaining.events_remaining, 0) AS events_remaining
    FROM district_rankings
    LEFT JOIN events_remaining ON district_rankings.team_key = events_remaining.team_key AND district_rankings.district_key = events_remaining.district_key
    LEFT JOIN impact_award_winners ON district_rankings.team_key = impact_award_winners.team_key
    WHERE impact_award_winners.team_key IS NULL
)