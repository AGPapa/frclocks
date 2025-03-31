CREATE TABLE IF NOT EXISTS district_rankings_without_qualify_awards AS (
    SELECT
        adjusted_district_rankings.team_key,
        adjusted_district_rankings.district_key,
        adjusted_district_rankings.rank,
        adjusted_district_rankings.points,
        adjusted_district_rankings.rookie_bonus,
        adjusted_district_rankings.events_remaining,
        adjusted_district_rankings.quals_remaining,
        adjusted_district_rankings.selections_remaining,
        adjusted_district_rankings.double_elims_remaining,
        adjusted_district_rankings.finals_remaining,
        adjusted_district_rankings.awards_remaining,
        adjusted_district_rankings.dcmp_semi_finals_remaining,
        ROW_NUMBER() OVER (PARTITION BY adjusted_district_rankings.district_key ORDER BY adjusted_district_rankings.points DESC, adjusted_district_rankings.rank ASC) AS active_team_rank
    FROM adjusted_district_rankings
    LEFT JOIN qualifying_award_winners ON adjusted_district_rankings.district_key = qualifying_award_winners.district_key
        AND adjusted_district_rankings.team_key = qualifying_award_winners.team_key
    WHERE qualifying_award_winners.team_key IS NULL
)