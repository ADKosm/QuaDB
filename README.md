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

##### Now all transactions run paralelly

So, you can open many telnets and concurrently run transactions.

You must begin transaction with `begin`, after that you should write all queries in current transaction (note: you don't see any results, before you commit transaction).
To compute transaction and commit it, write `commit`.

To test this mechanism, you can run `ruby insert.rb`. This script runs 200 transactions with insertions concurrently, so after some seconds, all insertions will be correctly applied.
To check, open telnet and run select transaction:

```$xslt
begin
select * from abc
commit
```