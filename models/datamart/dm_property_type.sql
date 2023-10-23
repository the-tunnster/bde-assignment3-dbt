CREATE VIEW dm_property_type AS
SELECT
    property_type,
    room_type,
    accommodates,
    DATE_TRUNC('month', current_date) AS month_year,
    -- Similar calculations from the previous view, adjusted for the different grouping
FROM facts_listings
GROUP BY property_type, room_type, accommodates
ORDER BY property_type, room_type, accommodates, month_year;
