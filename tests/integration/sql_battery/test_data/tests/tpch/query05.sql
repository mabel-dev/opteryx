select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    testdata.tpch_tiny.customer,
    testdata.tpch_tiny.orders,
    testdata.tpch_tiny.lineitem,
    testdata.tpch_tiny.supplier,
    testdata.tpch_tiny.nation,
    testdata.tpch_tiny.region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AFRICA'
    and o_orderdate >= '1993-01-01'
    and o_orderdate < '1994-01-01'
group by
    n_name
order by
    revenue desc;