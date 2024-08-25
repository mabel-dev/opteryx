select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            year(l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            testdata.tpch_tiny.supplier,
            testdata.tpch_tiny.lineitem,
            testdata.tpch_tiny.orders,
            testdata.tpch_tiny.customer,
            testdata.tpch_tiny.nation n1,
            testdata.tpch_tiny.nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'KENYA' and n2.n_name = 'PERU')
                or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')
            )
            and l_shipdate between '1995-01-01' and '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;