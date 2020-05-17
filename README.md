# 5300-Echidna
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

### Sprint Verano: Milestone 1
A simple SQL interpreter that runs from command line and prompts users for SQL statements.

* **Command to run**</br>
./sql5300 ../data</br>
enter SQL statement to validate and interpret</br>
enter "quit" to exit the program</br>

* **Current status**</br>
Support CREATE and SELECT statements</br>
Support TEXT, INT, DOUBLE data types</br>
Support FROM, WHERE, and ON clauses </br>
Support JOIN, LEFT JOIN, and RIGHT JOIN clauses</br>
Support AS alias</br>

* **Use case examples**</br>
SQL> create table foo (a text, b integer, c double)</br>
---> CREATE TABLE foo (a TEXT, b INT, c DOUBLE)</br>
SQL> select * from foo left join goober on foo.x=goober.x</br>
---> SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x</br>
SQL> select * from foo as f left join goober on f.x = goober.x</br>
---> SELECT * FROM foo AS f LEFT JOIN goober ON f.x = goober.x</br>
SQL> select * from foo as f left join goober as g on f.x = g.x</br>
---> SELECT * FROM foo AS f LEFT JOIN goober AS g ON f.x = g.x</br>
SQL> select a,b,g.c from foo as f, goo as g</br>
---> SELECT a, b, g.c FROM goo AS g, foo AS f</br>
SQL> select a,b,c from foo where foo.b > foo.c + 6</br>
---> SELECT a, b, c FROM foo WHERE foo.b > foo.c + 6</br>
SQL> select f.a,g.b,h.c from foo as f join goober as g on f.id = g.id where f.z >1</br>
---> SELECT f.a, g.b, h.c FROM foo AS f JOIN goober AS g ON f.id = g.id WHERE f.z > 1</br>
SQL> foo bar blaz</br>
 X Invalid SQL statement.</br>
SQL> quit</br>

### Sprint Verano: Milestone 2
A rudimentary heap storage engine implemented with SlottedPage, HeapFile, and HeapTable.

* **Command to run**</br>
./sql5300 ../data</br>
enter "test" to evaluate storage engine</br>
enter "quit" to exit the program</br>

* **Current status**</br>
SlottedPage - implementation done</br>
HeapFile - implementation done </br>
HeapTable  - implementation done</br>

* **Handover video**</br>
https://seattleu.instructuremedia.com/embed/c0dce768-704d-4164-b983-77114b32843e</br>

------------------------------------------------------------------------------------</br>

### Sprint Otono: Milestone 3 and Milestone 4

### Milestone 3
A rudimentary implementation of CREATE TABLE, DROP TABLE, SHOW TABLES and SHOW COLUMNS for a Schema Storage
* Implemented functions on SQLExec.cpp for this milestone.</br>
* run on cs1</br>

* **How to run program with example commands**</br>
./sql5300 ../data</br>
(sql5300: running with database environment at /Users/klundeen/cpsc5300/data)</br>
//run the following commands</br>
SQL> show tables</br>
SQL> show columns from _tables</br>
SQL> show columns from _columns</br>
SQL> create table foo (id int, data text, x integer, y integer, z integer)</br>
SQL> create table foo (goober int)</br>
SQL> create table goo (x int, x text)</br>
SQL> show tables</br>
SQL> show columns from foo</br>
SQL> drop table foo</br>
SQL> show tables</br>
SQL> show columns from foo</br>
SQL> quit</br>

### Milestone 4
Setup Indexing by implementing by creating, dropping, showing an index as well as dropping an index for each table.</br>
* Implemented required functions on SQLExec.cpp for this milestone.</br>
* Run on cs1 </br>
* HandOver Video</br>
https://seattleu.instructuremedia.com/embed/61f79262-502e-40d2-8ccc-99ddbc8c8d67</br>

* **How to run program with example commands**</br>
./sql5300 ../data</br>
(sql5300: running with database environment at /Users/klundeen/cpsc5300/data)</$
//run the following commands</br>
SQL> create table goober (x int, y int, z int)</br>
SQL> show tables</br>
SQL> show columns from goober</br>
SQL> create index fx on goober (x,y)</br>
SQL> show index from goober</br>
SQL> drop index fx from goober</br>
SQL> show index from goober</br>
SQL> create index fx on goober (x)</br>
SQL> show index from goober</br>
SQL> create index fx on goober (y,z)</br>
SQL> show index from goober</br>
SQL> create index fyz on goober (y,z)</br>
SQL> show index from goober</br>
SQL> drop index fx from goober</br>
SQL> show index from goober</br>
SQL> drop index fyz from goober</br>
SQL> show index from goober</br>
