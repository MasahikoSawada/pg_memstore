/* pg_memstore */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_memstore" to load this file. \quit

CREATE FUNCTION set(
key bytea,
value jsonb)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION get(key bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION delete(key bytea)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

/*
 * These two functions cannot work fine due to a bug in radixtree.h.
 * The issue is already reported:
 * https://www.postgresql.org/message-id/CAD21AoBB2U47V%3DF%2BwQRB1bERov_of5%3DBOZGaybjaV8FLQyqG3Q%40mail.gmail.com
 *
 * Once this bug is fixed, we can uncomment to support them.
*/

/*
CREATE FUNCTION list(
key OUT bytea,
value OUT jsonb)
RETURNS setof record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION save()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;
*/

CREATE FUNCTION load()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION memstore.memory_usage()
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;
