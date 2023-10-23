CREATE VIEW dm_host_neighbourhood AS
SELECT
    CASE 
        WHEN host_neighbourhood = 'Bondi' THEN 'Waverley'
        -- Additional mappings can be added here
    END AS host_neighbourhood_lga,
    DATE_TRUNC('month', current_date) AS month_year,
    COUNT(DISTINCT host_id) AS distinct_hosts,
    SUM((30 - availability_next_30_days) * price) AS estimated_revenue,
    -- Calculate revenue per host, and other metrics as needed
FROM facts_listings
GROUP BY host_neighbourhood_lga
ORDER BY host_neighbourhood_lga, month_year;
