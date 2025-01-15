/*
We've moved the filter on o_comment to a the WHERE clause from the JOIN clause.

ORIGINAL:

select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            testdata.tpch_tiny.customer left outer join testdata.tpch_tiny.orders on
                c_custkey = o_custkey
                and o_comment not like '%unusual%accounts%'
        group by
            c_custkey
    ) c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
*/

SELECT 
  c_count, 
  Count(*) AS custdist 
FROM 
  (
    SELECT 
      c_custkey, 
      Count(o_orderkey) AS c_count 
    FROM 
      testdata.tpch_tiny.customer 
      LEFT OUTER JOIN testdata.tpch_tiny.orders ON c_custkey = o_custkey 
    WHERE 
      o_comment NOT LIKE '%unusual%accounts%' 
    GROUP BY 
      c_custkey
  ) c_orders 
GROUP BY 
  c_count 
ORDER BY 
  custdist DESC, 
  c_count DESC;
