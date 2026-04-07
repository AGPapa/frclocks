CREATE TABLE IF NOT EXISTS teams_to_pass_v2 AS (
    SELECT
        district_rankings_without_qualify_awards.district_key,
        district_rankings_without_qualify_awards.team_key,
        district_rankings_without_qualify_awards.points,
        district_rankings_without_qualify_awards.rank,
        district_rankings_without_qualify_awards.active_team_rank,
        wcmp_spots.wcmp_spots - district_rankings_without_qualify_awards.active_team_rank + 1 AS teams_to_pass
    FROM district_rankings_without_qualify_awards
    JOIN wcmp_spots_v2 AS wcmp_spots ON district_rankings_without_qualify_awards.district_key = wcmp_spots.district_key
    WHERE wcmp_spots.wcmp_spots - district_rankings_without_qualify_awards.active_team_rank + 1 > 0
)