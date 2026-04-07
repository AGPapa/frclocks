CREATE TABLE IF NOT EXISTS wcmp_spots AS (
    WITH
    bonus_point_spots AS (
        SELECT
            qualifying_award_winners.district_key,
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
        FROM qualifying_award_winners
        GROUP BY qualifying_award_winners.district_key
    ),
    dcmp_events_needing_ras AS (
        SELECT
            event_states.district_key,
            event_states.event_key
        FROM event_states
        WHERE event_states.event_type = 'District Championship'
            AND event_states.event_state IN ('Completed', 'Awards')
    ),
    dcmp_events_with_ras AS (
        SELECT DISTINCT
            events.district_key,
            awards.event_key
        FROM awards
        JOIN events ON awards.event_key = events.event_key
        WHERE events.event_type = 'District Championship'
            AND awards.award_type = 10
    ),
    missing_qualifying_awards AS (
        SELECT
            dcmp_events_needing_ras.district_key,
            COUNT(*) AS num_missing_qualifying_awards
        FROM dcmp_events_needing_ras
        LEFT JOIN dcmp_events_with_ras
            ON dcmp_events_needing_ras.district_key = dcmp_events_with_ras.district_key
            AND dcmp_events_needing_ras.event_key = dcmp_events_with_ras.event_key
        WHERE dcmp_events_with_ras.event_key IS NULL
            AND dcmp_events_needing_ras.district_key != '2025fsc'
        GROUP BY dcmp_events_needing_ras.district_key
    )
    SELECT
        district_lookup.district_key,
        district_lookup.wcmp_capacity
            - district_lookup.dcmp_impact_awards
            - district_lookup.dcmp_ei_awards
            - district_lookup.dcmp_ras_awards
            - COALESCE(bonus_point_spots.num_dcmp_winners, CASE WHEN district_lookup.district_key = '2025fsc' THEN 2 ELSE 4 END) -- Assume there will be a backup team on the winning alliance unless we know otherwise. South Carolina only sends 2 winners
            + COALESCE(bonus_point_spots.num_double_qualifying_awards, 0) -- If a winner also gets a qualifying award, then an extra point spot is available
            + 2 * COALESCE(bonus_point_spots.num_triple_qualifying_awards, 0) -- If a prequalified also gets two qualifying awards, then an two extra point spots are available
            + COALESCE(missing_qualifying_awards.num_missing_qualifying_awards, 0) -- If RAS was not awarded at a completed DCMP event, then an extra point spot is available
        AS wcmp_spots
    FROM district_lookup
    LEFT JOIN bonus_point_spots ON district_lookup.district_key = bonus_point_spots.district_key
    LEFT JOIN missing_qualifying_awards ON district_lookup.district_key = missing_qualifying_awards.district_key
)