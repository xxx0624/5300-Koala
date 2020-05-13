# 5300-Koala
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

Parts of `Tables` and `Indices` reside in `SQLExec.cpp` and `scheme_tables.cpp`. Most of our work are in `SQLExec.cpp` and all functions related in this milesone are explained well by function name and comments.

## Milestone3
In this `Milestone3`, we support following operations:
- CREATE TABLE
- DROP TABLE
- SHOW TABLES
- SHOW COLUMNS

### Example scripts for `Milestone3`
```
show tables

show columns from _tables

show columns from _columns

create table foo (id int, data text, x integer, y integer, z integer)

show columns from foo

drop table foo
```

## Milestone4
In this `Milestone4`, we support following operations:
- CREATE INDEX
- SHOW INDEX
- DROP INDEX

### Example scripts for `Milestone4`
```
show columns from _indeices

create table foo (id int, data text, x integer, y integer, z integer)

show tables

create index fx on foo (x,y)

show index from foo

drop index fx from foo
```