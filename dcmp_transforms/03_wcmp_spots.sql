CREATE TABLE IF NOT EXISTS wcmp_spots AS (
    WITH
    bonus_point_spots AS (
        SELECT
            event_states.district_key,
            SUM(CASE WHEN qualifying_award_winners.is_winner THEN 1 ELSE 0 END) AS num_dcmp_winners,
            SUM(CASE WHEN 
                (
                    CASE WHEN qualifying_award_winners.award_type IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN qualifying_award_winners.is_prequalified THEN 1 ELSE 0 END
                    + CASE WHEN qualifying_award_winners.is_winner THEN 1 ELSE 0 END
                ) = 2 THEN 1 ELSE 0 END)
            AS num_double_qualifying_awards,
            SUM(CASE WHEN 
                (
                    CASE WHEN qualifying_award_winners.award_type IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN qualifying_award_winners.is_prequalified THEN 1 ELSE 0 END
                    + CASE WHEN qualifying_award_winners.is_winner THEN 1 ELSE 0 END
                ) = 3 THEN 1 ELSE 0 END)
            AS num_triple_qualifying_awards
        FROM event_states
        JOIN qualifying_award_winners ON event_states.district_key = qualifying_award_winners.district_key
        WHERE event_states.event_type = 'District Championship'
        AND event_states.event_state IN ('Completed', 'Awards')
        GROUP BY event_states.district_key
    )
    SELECT
        district_lookup.district_key,
        district_lookup.wcmp_capacity
            - district_lookup.dcmp_impact_awards
            - district_lookup.dcmp_ei_awards
            - district_lookup.dcmp_ras_awards
            - COALESCE(bonus_point_spots.num_dcmp_winners, 4) -- Assume there will be a backup team on the winning alliance unless we know otherwise
            + COALESCE(bonus_point_spots.num_double_qualifying_awards, 0) -- If a winner also gets a qualifying award, then an extra point spot is available
            + 2 * COALESCE(bonus_point_spots.num_triple_qualifying_awards, 0) -- If a prequalified also gets two qualifying awards, then an two extra point spots are available
        AS wcmp_spots
    FROM district_lookup
    LEFT JOIN bonus_point_spots ON district_lookup.district_key = bonus_point_spots.district_key
)