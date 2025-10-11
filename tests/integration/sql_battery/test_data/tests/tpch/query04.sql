/*
We've replaced the EXISTS clause will a LEFT SEMI JOIN

ORIGINAL:

select
    o_orderpriority,
    count(*) as order_count
from
    testdata.tpch_tiny.orders as o
where
    o_orderdate >= '1996-05-01'
    and o_orderdate < '1996-08-01'
    and exists (
        select
            *
        from
            testdata.tpch_tiny.lineitem
        where
            l_orderkey = o.o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;
*/

SELECT 
  o_orderpriority, 
  Count(*) AS order_count 
FROM 
  testdata.tpch_tiny.orders AS o LEFT semi 
  JOIN (
    SELECT 
      * 
    FROM 
      testdata.tpch_tiny.lineitem AS l 
    WHERE 
      l_commitdate < l_receiptdate
  ) AS l ON l.l_orderkey = o.o_orderkey 
WHERE 
  o_orderdate >= '1996-05-01' 
  AND o_orderdate < '1996-08-01' 
GROUP BY 
  o_orderpriority 
ORDER BY 
  o_orderpriority;
