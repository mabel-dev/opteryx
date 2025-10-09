select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    testdata.tpch_tiny.lineitem,
    testdata.tpch_tiny.part
where
    l_partkey = p_partkey
    and l_shipdate >= '1995-08-01'
    and l_shipdate < '1995-09-01';