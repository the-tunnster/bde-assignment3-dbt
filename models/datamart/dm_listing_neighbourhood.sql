CREATE VIEW dm_listing_neighbourhood AS
SELECT
    listing_neighbourhood,
    DATE_TRUNC('month', current_date) AS month_year,
    (COUNT(CASE WHEN has_availability = 't' THEN 1 END)::FLOAT / COUNT(*)::FLOAT) * 100 AS active_listings_rate,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    MEDIAN(price) AS median_price,
    AVG(price) AS avg_price,
    COUNT(DISTINCT host_id) AS distinct_hosts,
    (COUNT(DISTINCT CASE WHEN is_superhost = 't' THEN host_id END)::FLOAT / COUNT(DISTINCT host_id)::FLOAT) * 100 AS superhost_rate,
    AVG(review_count) AS avg_review_scores_rating_for_active_listings,
    -- Additional calculations for percentage changes and other metrics can be added here
FROM facts_listings
GROUP BY listing_neighbourhood
ORDER BY listing_neighbourhood, month_year;
