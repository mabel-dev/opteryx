# Common Table Expressions (CTEs)

## Sub Queries
~~~sql
WITH cte_sales AS (
    SELECT 
        staff_id, 
        COUNT(*) AS order_count  
    FROM
        sales.orders
    WHERE 
        YEAR(order_date) = 2018
    GROUP BY
        staff_id

)
SELECT
    AVG(order_count)
    average_orders_by_staff
FROM 
    cte_sales;
~~~

## Ad Hoc Tables

~~~sql
WITH rating_priorities AS (
    (
        {"rating": "CRITICAL", "priority":5},
        {"rating": "HIGH", "priority":4},
        {"rating": "MEDIUM", "priority":3},
        {"rating": "LOW", "priority":2},
        {"rating": "INFO", "priority":1},
    )
)

SELECT
    findings.Rating,
    COUNT(*) AS FindingsCount
FROM
    findings,
    rating_priorities
WHERE
    findings.Rating = rating_priorities.rating
GROUP BY
    findings.Rating
ORDER BY
    rating_priorities.priority DESC
~~~