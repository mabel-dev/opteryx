/* 
Opteryx syntax changes
- view definitions changed to CTE
*/
with revenue_cached as
(select
    l_suppkey as supplier_no,
    sum(l_extendedprice * (1 - l_discount)) as total_revenue
from
    testdata.tpch_tiny.lineitem
where
    l_shipdate >= '1996-01-01'
    and l_shipdate < '1996-04-01'
group by l_suppkey)

, max_revenue_cached as
(select
    max(total_revenue) as max_revenue
from
    revenue_cached)

select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    testdata.tpch_tiny.supplier,
    revenue_cached,
    max_revenue_cached
where
    s_suppkey = supplier_no
    and total_revenue = max_revenue 
order by s_suppkey;