# 5300-Echidna
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

### Sprint Verano: Milestone 1
A simple SQL interpreter that runs from command line and prompts users for SQL statements.

* **Command to run**
./sql5300 ../data
enter SQL statement to validate and interpret
enter "quit" to exit the program

* **Current status**
Support CREATE and SELECT statements
Support TEXT, INT, DOUBLE data types
Support FROM, WHERE, and ON clauses 
Support JOIN, LEFT JOIN, and RIGHT JOIN clauses
Support AS alias

* **Use case examples**
SQL> create table foo (a text, b integer, c double)
---> CREATE TABLE foo (a TEXT, b INT, c DOUBLE)
SQL> select * from foo left join goober on foo.x=goober.x
---> SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x
SQL> select * from foo as f left join goober on f.x = goober.x
---> SELECT * FROM foo AS f LEFT JOIN goober ON f.x = goober.x
SQL> select * from foo as f left join goober as g on f.x = g.x
---> SELECT * FROM foo AS f LEFT JOIN goober AS g ON f.x = g.x
SQL> select a,b,g.c from foo as f, goo as g
---> SELECT a, b, g.c FROM goo AS g, foo AS f
SQL> select a,b,c from foo where foo.b > foo.c + 6
---> SELECT a, b, c FROM foo WHERE foo.b > foo.c + 6
SQL> select f.a,g.b,h.c from foo as f join goober as g on f.id = g.id where f.z >1
---> SELECT f.a, g.b, h.c FROM foo AS f JOIN goober AS g ON f.id = g.id WHERE f.z > 1
SQL> foo bar blaz
 X Invalid SQL statement.
SQL> quit

### Sprint Verano: Milestone 2
A rudimentary heap storage engine implemented with SlottedPage, HeapFile, and HeapTable.

* **Command to run**
./sql5300 ../data
enter "test" to evaluate storage engine
enter "quit" to exit the program

* **Current status**
SlottedPage - implementation done
HeapFile - implementation done 
HeapTable  - implementation done

* **Handover video**

