/* 
Opteryx syntax changes
- temporary table definition changed to CTE
*/

with l3 as 
(select l_orderkey, count(distinct l_suppkey) as cntSupp
from testdata.tpch_tiny.lineitem
where l_receiptdate > l_commitdate and l_orderkey is not null
group by l_orderkey
having cntSupp = 1
)

, location as (
select supplier.* from testdata.tpch_tiny.supplier, testdata.tpch_tiny.nation where
s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'
)
select s_name, count(*) as numwait
from
(
select li.l_suppkey, li.l_orderkey
from testdata.tpch_tiny.lineitem li join testdata.tpch_tiny.orders o on li.l_orderkey = o.o_orderkey and
                      o.o_orderstatus = 'F'
     join
     (
     select l_orderkey, count(distinct l_suppkey) as cntSupp
     from testdata.tpch_tiny.lineitem
     group by l_orderkey
     ) l2 on li.l_orderkey = l2.l_orderkey and 
             li.l_receiptdate > li.l_commitdate and 
             l2.cntSupp > 1
) l1 join l3 on l1.l_orderkey = l3.l_orderkey
 join location s on l1.l_suppkey = s.s_suppkey
group by
 s_name
order by
 numwait desc,
 s_name
limit 100;