/* 
Opteryx syntax changes
- view definitions changed to CTE
*/

with q22_customer_tmp_cached as
(select
    c_acctbal,
    c_custkey,
    substring(c_phone, 1, 2) as cntrycode
from
    testdata.tpch_tiny.customer
where
    substring(c_phone, 1, 2) = '13' or
    substring(c_phone, 1, 2) = '31' or
    substring(c_phone, 1, 2) = '23' or
    substring(c_phone, 1, 2) = '29' or
    substring(c_phone, 1, 2) = '30' or
    substring(c_phone, 1, 2) = '18' or
    substring(c_phone, 1, 2) = '17'
),
 
q22_customer_tmp1_cached as
(select
    avg(c_acctbal) as avg_acctbal
from
    q22_customer_tmp_cached
where
    c_acctbal > 0.00
),

q22_orders_tmp_cached as
(select
    o_custkey
from
    testdata.tpch_tiny.orders
group by
    o_custkey)

select
    cntrycode,
    count(1) as numcust,
    sum(c_acctbal) as totacctbal
from (
    select
        cntrycode,
        c_acctbal,
        avg_acctbal
    from
        q22_customer_tmp1_cached ct1 join (
            select
                cntrycode,
                c_acctbal
            from
                q22_orders_tmp_cached ot
                right outer join q22_customer_tmp_cached ct
                on ct.c_custkey = ot.o_custkey
            where
                o_custkey is null
        ) ct2
) a
where
    c_acctbal > avg_acctbal
group by
    cntrycode
order by
    cntrycode;