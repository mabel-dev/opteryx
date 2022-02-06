# SQL Examples

Opteryx has a set on built-in datasets for demonstration and testing.

- $satellites
- $planets

~~~sql
SELECT *
  FROM $planets
~~~

~~~
+-----------------------+
| LastName   | SchoolID |
+-----------------------+
| Adams      | 50       |
| Buchanan   | 52       |
| Coolidge   | 52       |
| Davis      | 51       |
| Eisenhower | 77       |
+-----------------------+
~~~