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
https://seattleu.instructuremedia.com/embed/c0dce768-704d-4164-b983-77114b32843e