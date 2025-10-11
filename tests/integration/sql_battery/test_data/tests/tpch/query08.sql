select
    o_year,
    sum(case
        when nation = 'PERU' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            year(o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            testdata.tpch_tiny.part,
            testdata.tpch_tiny.supplier,
            testdata.tpch_tiny.lineitem,
            testdata.tpch_tiny.orders,
            testdata.tpch_tiny.customer,
            testdata.tpch_tiny.nation n1,
            testdata.tpch_tiny.nation n2,
            testdata.tpch_tiny.region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between '1995-01-01' and '1996-12-31'
            and p_type = 'ECONOMY BURNISHED NICKEL'
    ) as all_nations
group by
    o_year
order by
    o_year;