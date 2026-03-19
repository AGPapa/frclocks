CREATE TABLE IF NOT EXISTS teams_to_pass AS (
    WITH num_impact_award_winners AS (
        SELECT
            impact_award_winners.district_key,
            impact_award_winners.region,
            COUNT(*) AS num_impact_award_winners
        FROM impact_award_winners
        GROUP BY impact_award_winners.district_key, impact_award_winners.region
    )
    SELECT
        district_rankings_without_impact.district_key,
        district_rankings_without_impact.region,
        district_rankings_without_impact.team_key,
        district_rankings_without_impact.points,
        district_rankings_without_impact.inflated_points,
        district_rankings_without_impact.rank,
        district_rankings_without_impact.active_team_rank,
        (district_lookup.dcmp_capacity - COALESCE(num_impact_award_winners.num_impact_award_winners, 0) - district_points_remaining.events_remaining) - district_rankings_without_impact.active_team_rank + 1 AS teams_to_pass
    FROM district_rankings_without_impact
    JOIN district_lookup ON district_rankings_without_impact.district_key = district_lookup.district_key
    LEFT JOIN num_impact_award_winners ON district_rankings_without_impact.district_key = num_impact_award_winners.district_key AND district_rankings_without_impact.region = num_impact_award_winners.region
    JOIN district_points_remaining ON district_rankings_without_impact.district_key = district_points_remaining.district_key AND district_rankings_without_impact.region = district_points_remaining.region
    WHERE (district_lookup.dcmp_capacity - COALESCE(num_impact_award_winners.num_impact_award_winners, 0) - district_points_remaining.events_remaining) - district_rankings_without_impact.active_team_rank + 1 > 0
)