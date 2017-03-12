# QuaDB

Unfortunatly, this DB can't create new tables yet. 
Therefore, to use this DB, you need to create in /tmp/simpledb at least 2 files: tableName.meta and tableName.data.
 
 In tableName.meta you must write scheme of tableName, for example:
 
 ```
id;int
name;varchar;10
dt;datetime
```

You can run ``python3 createdb.py`` or `` python3 createdb.py empty `` in order to automatically create .meta and .data files, which are describe example table abc.

## Available queries

##### Begin and commit transaction

Important! If you want to save your insertion data, you must run command ``begin`` before all commands, and run command ``commit`` after all other insert commands. Otherwise, after reloading server, all uncommited data will be deleted.

##### Show tables

Query example: ``show tables``

Result: show all tables, which is available (is in the /tmp/simpledb)

##### Describe table

Query example: ``describe table table1``

Result: show structure of table1

##### Select data from tables

Query example: ``select * from table1``

Result: show all records in table1

##### Insert data into table

Query example: ``insert into table1 values (15,hello,NOW)``

Result: if table1 have scheme as is in example above, this query add new record into table1 with id = 15, name = hello and dt = current time

##### Delete data from table

Query example: ``delete from abc where id < 10``

Result: remove all records, which id is less then 10

