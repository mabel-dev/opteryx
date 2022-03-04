# SQL Statements

~~~sql
SELECT [DISTINCT] <select_expression>
  FROM <table_list>
   FOR <temporal_expression>
 WHERE <where_expression>
 GROUP BY <group_by_expression>
HAVING <having_expression>
 LIMIT <n>
OFFSET <n>
 ORDER BY <field_list> [ASC|DESC]
~~~

## Table Value Constructor
~~~sql
SELECT * 
  FROM (VALUES ('High', 3),('Medium', 2),('Low', 1)) AS ratings(name, rating)
~~~

~~~sql
SELECT *
  FROM UNNEST(('High', 'Medium', 'Low')) as ratings
~~~