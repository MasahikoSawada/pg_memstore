create extension pg_memstore;

select memstore.set('key-1', '{"abc" : 1}');
select memstore.get('key-1');
select memstore.set('key-1', '{"abc" : 999}');
select memstore.get('key-1');

select memstore.set('123456789', '{"b" : 1}'); -- truncate the key
select memstore.get('12345678');

select memstore.set('', '{"a":true}'); -- error

-- switch to another session
\c -
select * from memstore.list();
select memstore.set('key-3', '{"abc": [1,2,3]}');
select memstore.get('key-3');
select memstore.delete('key-1');
select * from memstore.list();
