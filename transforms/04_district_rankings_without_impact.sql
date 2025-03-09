CREATE TABLE IF NOT EXISTS district_rankings_without_impact AS (
    SELECT
        adjusted_district_rankings.team_key,
        adjusted_district_rankings.district_key,
        adjusted_district_rankings.rank,
        adjusted_district_rankings.points,
        adjusted_district_rankings.rookie_bonus,
        adjusted_district_rankings.inflated_points,
        adjusted_district_rankings.events_remaining,
        ROW_NUMBER() OVER (PARTITION BY adjusted_district_rankings.district_key ORDER BY adjusted_district_rankings.inflated_points DESC, adjusted_district_rankings.rank ASC) AS active_team_rank
    FROM adjusted_district_rankings
    LEFT JOIN impact_award_winners ON adjusted_district_rankings.team_key = impact_award_winners.team_key
    WHERE impact_award_winners.team_key IS NULL
)