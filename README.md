# pg_memstore

A simple in-memory key-value store extension.

- Keys are encoded to 8-bytes integer and values are `jsonb` data.
- The in-memory store is created using DSA so the store is shared among all postgres processes.
- Support basic SET, GET, DELETE, and LIST operations.
- Support to dump all key-value pairs to the disk and load it.
- Support WAL-logging so replicas can update its store.
- No trnsactional control
- No access control

`pg_memstore` requies PostgreSQL 17 or higher.

## **CAUTION!!**

These fuctions are diabled due to a bug in radixtree.h

- `memstore.save()`
- `memstore.list()`

In order to make it work, [the patch](https://www.postgresql.org/message-id/CAD21AoBB2U47V%3DF%2BwQRB1bERov_of5%3DBOZGaybjaV8FLQyqG3Q%40mail.gmail.com) needs to be applied to PostgreSQL code.

# Installation

## Build from source code

```bash
$ make USE_PGXS=1
# make USE_PGXS=1 install
```

## Register `pg_memstore` extension

```
$ vi $PGDATA/postgresql.conf
shared_preload_libraries = 'pg_memstore'
$ pg_ctl start
$ psql

=# CREATE EXTENSION pg_memstore;
CREATE EXTENSION

=# \df memstore.*
                                 List of functions
  Schema  |     Name     | Result data type |      Argument data types       | Type
----------+--------------+------------------+--------------------------------+------
 memstore | delete       | boolean          | key bytea                      | func
 memstore | get          | jsonb            | key bytea                      | func
 memstore | list         | SETOF record     | OUT key bytea, OUT value jsonb | func
 memstore | load         | boolean          |                                | func
 memstore | memory_usage | bigint           |                                | func
 memstore | save         | boolean          |                                | func
 memstore | set          | boolean          | key bytea, value jsonb         | func
(7 rows)
```

# Examples

## Basic operations

`pg_memstore` supports basic SET, GET, and LIST operations:

```sql
=# SELECT memstore.set('key-1', '{"a": 1}'); -- return false if a new key
 set
-----
 f
(1 row)

=# SELECT memstore.get('key-1');
   get
----------
 {"a": 1}
(1 row)

=# SELECT memstore.set('key-1', '{"a": 999}'); -- update the value
 set
-----
 t
(1 row)

=# SELECT memstore.get('key-1');
    get
------------
 {"a": 999}
(1 row)

=# SELECT memstore.set('key-2', '{"b": [1, 2, 3]}');
 set
-----
 f
(1 row)

=# SELECT * from memstore.list();
      key       |      value
----------------+------------------
 \x6b65792d317e | {"a": 999}
 \x6b65792d327e | {"b": [1, 2, 3]}
(2 rows)

=# SELECT memstore.delete('key-1');
 delete 
--------
 t
(1 row)

=# SELECT * from memstore.list();
       key      |      value
----------------+------------------
 \x6b65792d327e | {"b": [1, 2, 3]}
(1 row)

=# SELECT memstore.memory_usage();
 memory_usage
--------------
       262144
(1 row)
```

## Dump and restore

`pg_memstore` can save and load its contents to/from the disk:

```sql
=# SELECT * from memstore.list();
       key      |      value
----------------+------------------
 \x6b65792d317e | {"a": 999}
 \x6b65792d327e | {"b": [1, 2, 3]}
(2 rows)

=# SELECT memstore.save();
 save
------
 t
(1 row)
```

Restart the server and load the dump file:

```bash
$ pg_ctl retart
```

```sql
=# SELECT * from memstore.list();
 key | value
-----+-------
(0 rows)

=# SELECT memstore.load();
NOTICE:  loading 2 entries...
 load
------
 t
(1 row)

=# SELECT * from memstore.list();

       key      |      value
----------------+------------------
 \x6b65792d317e | {"a": 999}
 \x6b65792d327e | {"b": [1, 2, 3]}
(2 rows)
```

## Replication

If `pg_memstore.wal_logging` is enabled, all `memstore.set()` operations are WAL-logged. Streaming replicaiton replicas can replay these changes in its store.
