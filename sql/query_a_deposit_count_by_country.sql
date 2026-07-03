SELECT
    c.country,
    COUNT(d.deposit_id) AS deposit_count
FROM dim_client_current AS c
LEFT JOIN fact_deposit AS d
    ON d.client_id = c.client_id
   AND LOWER(d.deposit_status) = 'completed'
GROUP BY c.country
ORDER BY
    CASE WHEN COUNT(d.deposit_id) = 0 THEN 0 ELSE 1 END,
    COUNT(d.deposit_id) DESC,
    c.country;
