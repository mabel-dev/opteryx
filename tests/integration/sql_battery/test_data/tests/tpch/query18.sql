/* 
Opteryx syntax changes
- view definitions changed to CTE
*/
with q18_tmp_cached as
(select
    l_orderkey,
    sum(l_quantity) as t_sum_quantity
from
    testdata.tpch_tiny.lineitem
where
    l_orderkey is not null
group by
    l_orderkey)

select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    testdata.tpch_tiny.customer,
    testdata.tpch_tiny.orders,
    q18_tmp_cached t,
    testdata.tpch_tiny.lineitem l
where
    c_custkey = o_custkey
    and o_orderkey = t.l_orderkey
    and o_orderkey is not null
    and t.t_sum_quantity > 300
    and o_orderkey = l.l_orderkey
    and l.l_orderkey is not null
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate 
limit 100;