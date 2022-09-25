# NULL Semantics

Most comparisons to `NULL` return `NULL`.

~~~sql
WHERE a IS NOT b
~~~
and
~~~sql
WHERE NOT a IS b
~~~

Appear to be identical however, produce different results when `NULL` values are encountered. 