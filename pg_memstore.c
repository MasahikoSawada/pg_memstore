/* -------------------------------------------------------------------------
 *
 * pg_memstore.c
 *	A simple on-memory key-value store extension.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"

PG_MODULE_MAGIC;

#define RT_PREFIX jbstore
#define RT_SCOPE
#define RT_USE_DELETE
#define RT_DECLARE
#define RT_DEFINE
#define RT_SHMEM
#define RT_VALUE_TYPE Jsonb
#define RT_VARLEN_VALUE_SIZE(jb) (VARSIZE(jb))
#include "lib/radixtree.h"

#define MEMSTORE_DUMP_FILE	"memstore.data"
static const uint32 MEMSTORE_MAGIC_NUMBER = 0x20241218;

/* Struct for global shared state */
typedef struct PgMemStoreShmemCtlData
{
	void	*raw_dsa_area;
	jbstore_handle	handle;
} PgMemStoreShmemCtlData;

/* Collection of backend-local state */
typedef struct PgMemStoreLocalData
{
	PgMemStoreShmemCtlData	*shmem;
	dsa_area	*dsa;
	jbstore_radix_tree	*store;
} PgMemStoreLocalData;

/*
 * Struct to collect all key-value pairs. Used for
 * collect_all_key_values().
 */
typedef struct MemStoreListContext
{
	TupleDesc	tupdesc;
	int32		nentries;
	int32		max_nentries;
	uint64		*keys;
	Jsonb		**values;
} MemStoreListContext;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* WAL-logging support */

#define RM_MEMSTORERMGRS_ID           RM_EXPERIMENTAL_ID
#define MEMSTORERMGR_NAME            "memstore_rmgrs"

/* RMGR APIs */
void		memstorermgrs_redo(XLogReaderState *record);
void		memstorermgrs_desc(StringInfo buf, XLogReaderState *record);
const char *memstorermgrs_identify(uint8 info);

static const RmgrData memstore_rmgrs = {
	.rm_name = MEMSTORERMGR_NAME,
	.rm_redo = memstorermgrs_redo,
	.rm_desc = memstorermgrs_desc,
	.rm_identify = memstorermgrs_identify,
};

/* WAL record for "set" operation */
typedef struct xl_memstore_set
{
	uint64	key;
	Size	val_size;
	char	value[FLEXIBLE_ARRAY_MEMBER];
} xl_memstore_set;
#define SizeOfXLogMemStoreSet	(offsetof(xl_memstore_set, value))
#define XLOG_MEMSTORE_SET	0x10

static bool WalLogging = false;
static PgMemStoreLocalData PgMemStoreLocal;
static int PgMemStoreTrancheId; /* initialized at shmem-startup */

PG_FUNCTION_INFO_V1(set);
PG_FUNCTION_INFO_V1(get);
PG_FUNCTION_INFO_V1(delete);
PG_FUNCTION_INFO_V1(save);
PG_FUNCTION_INFO_V1(load);
PG_FUNCTION_INFO_V1(list);
PG_FUNCTION_INFO_V1(memory_usage);

static void memstore_shmem_request(void);
static void memstore_shmem_startup(void);
static void collect_all_key_values(MemStoreListContext *ctx);
static inline void ensure_attach_memstore_shmem(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("pg_memstore.wal_logging",
							 "Write WAL for updates.",
							 NULL,
							 &WalLogging,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = memstore_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = memstore_shmem_startup;

	RegisterCustomRmgr(RM_MEMSTORERMGRS_ID, &memstore_rmgrs);
}

/* Return the memory size required for dsa */
static Size
memstore_shmem_dsa_size(void)
{
	return MAXALIGN(256 * 1024);
}

/* Return the memory size required for shared memory */
static Size
memstore_shmem_size(void)
{
	Size	sz;

	sz = sizeof(PgMemStoreShmemCtlData);
	sz = add_size(sz, memstore_shmem_dsa_size());

	return sz;
}

static void
memstore_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(memstore_shmem_size());
	RequestNamedLWLockTranche("pg_memstore", 1);
}

static void
memstore_shmem_startup(void)
{
	bool	found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* Create and register own tranche-id */
	PgMemStoreTrancheId = LWLockNewTrancheId();
	LWLockRegisterTranche(PgMemStoreTrancheId, "pg_memstore_tranche");

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	PgMemStoreLocal.shmem = ShmemInitStruct("pg_memstore",
											memstore_shmem_size(),
											&found);

	LWLockRelease(AddinShmemInitLock);

	if (!found)
	{
		dsa_area	*dsa;
		jbstore_radix_tree *store;
		PgMemStoreShmemCtlData *ctl = PgMemStoreLocal.shmem;
		char	*p = (char *) ctl;

		/* the allocation of PgMemStoreLocal.shmem itself */
		p += MAXALIGN(memstore_shmem_size());

		/*
		 * Create a small dsa allocation in plain shared memory.
		 */
		ctl->raw_dsa_area = p;
		dsa = dsa_create_in_place(ctl->raw_dsa_area,
								  memstore_shmem_dsa_size(),
								  PgMemStoreTrancheId, 0);
		dsa_pin(dsa);

		/*
		 * To ensure radixtree is create in "plain" shared memory,
		 * temporarily limit size of dsa to the initialize size of
		 * the dsa.
		 */
		dsa_set_size_limit(dsa, memstore_shmem_dsa_size());

		store = jbstore_create(CurrentMemoryContext, dsa,
							   PgMemStoreTrancheId);
		ctl->handle = jbstore_get_handle(store);

		/* Lift limit set above */
		dsa_set_size_limit(dsa, -1);

		/*
		 * postmaster will never access these again, thus free the
		 * local references.
		 */
		jbstore_detach(store);
		dsa_detach(dsa);
	}
}

/* Attach to the shared memstore if not yet */
static inline void
ensure_attach_memstore_shmem(void)
{
	MemoryContext	old_ctx;

	if (unlikely(PgMemStoreLocal.shmem == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_memstore must be loaded via \"shared_preload_libraries\"")));

	if (likely(PgMemStoreLocal.store != NULL))
		return;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	PgMemStoreLocal.dsa = dsa_attach_in_place(PgMemStoreLocal.shmem->raw_dsa_area,
											  NULL);
	dsa_pin_mapping(PgMemStoreLocal.dsa);

	PgMemStoreLocal.store = jbstore_attach(PgMemStoreLocal.dsa,
										   PgMemStoreLocal.shmem->handle);

	MemoryContextSwitchTo(old_ctx);
}

/* Construct the key from the given bytea data 'in' */
static inline int64
make_key(bytea *in)
{
	uint64	key;
	int		key_len = VARSIZE_ANY_EXHDR(in);

	if (key_len == 0)
		elog(ERROR, "length of key must be more than 0");

	if (key_len > sizeof(key))
	{
		char	*str = text_to_cstring(in);
		int 	len = strlen(str);
		const int limit = sizeof(uint64);

		len = pg_mbcliplen(str, len, limit);
		 ereport(NOTICE,
				 (errcode(ERRCODE_NAME_TOO_LONG),
				  errmsg("key \"%s\" will be truncated to \"%.*s\"",
						 str, limit, str)));
	}

	memcpy(&key, VARDATA_ANY(in), sizeof(key));

	return key;
}

/*
 * "SET" operation.
 *
 * Return true if the given key already exists and we updated its value.
 *
 * If WAL-logging is enabled, it also writes the corresponding WAL.
 */
Datum
set(PG_FUNCTION_ARGS)
{
	bytea	*in = PG_GETARG_BYTEA_PP(0);
	Jsonb	*value = PG_GETARG_JSONB_P(1);
	uint64	key = make_key(in);
	bool 	found;

	ensure_attach_memstore_shmem();

	jbstore_lock_exclusive(PgMemStoreLocal.store);
	found = jbstore_set(PgMemStoreLocal.store, key, value);
	jbstore_unlock(PgMemStoreLocal.store);

	if (WalLogging)
	{
		xl_memstore_set xlrec;

		xlrec.key = key;
		xlrec.val_size = VARSIZE(value);

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfXLogMemStoreSet);
		XLogRegisterData((char *) value, VARSIZE(value));
		XLogInsert(RM_MEMSTORERMGRS_ID, XLOG_MEMSTORE_SET);
	}

	PG_RETURN_BOOL(found);
}

/*
 * "GET" operation.
 *
 * Return the value of the given key. Otherwise, return NULL>
 */
Datum
get(PG_FUNCTION_ARGS)
{
	bytea	*in = PG_GETARG_BYTEA_PP(0);
	Jsonb	*value;

	ensure_attach_memstore_shmem();

	jbstore_lock_share(PgMemStoreLocal.store);
	value = jbstore_find(PgMemStoreLocal.store, make_key(in));
	jbstore_unlock(PgMemStoreLocal.store);

	if (value == NULL)
		PG_RETURN_NULL();

	PG_RETURN_JSONB_P(value);
}

/*
 * "DELETE" operation.
 *
 * Return true if the given key existed and is removed.
 */
Datum
delete(PG_FUNCTION_ARGS)
{
	bytea	*in = PG_GETARG_BYTEA_PP(0);
	bool	found;

	ensure_attach_memstore_shmem();

	jbstore_lock_share(PgMemStoreLocal.store);
	found = jbstore_delete(PgMemStoreLocal.store, make_key(in));
	jbstore_unlock(PgMemStoreLocal.store);

	PG_RETURN_BOOL(found);
}

/*
 * Dump all key-value pairs to $PGDATA/MEMSTORE_MAGIC_NUMBER file.
 *
 * The dump file can be loaded via load().
 */
Datum
save(PG_FUNCTION_ARGS)
{
	FILE	   *file;
	MemStoreListContext *ctx;

	ensure_attach_memstore_shmem();

	/* collect all key-value pairs */
	ctx = palloc(sizeof(MemStoreListContext));
	ctx->nentries = 0;
	ctx->max_nentries = 128;
	ctx->keys = palloc(sizeof(uint64) * ctx->max_nentries);
	ctx->values = palloc(sizeof(Jsonb *) * ctx->max_nentries);
	collect_all_key_values(ctx);

	/* Allocate new data file */
	file = AllocateFile(MEMSTORE_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	/* write the magic number */
	if (fwrite(&MEMSTORE_MAGIC_NUMBER, sizeof(uint32), 1, file) != 1)
		goto error;

	/* write the total number of key-value pairs */
	if (fwrite(&(ctx->nentries), sizeof(int32), 1, file) != 1)
		goto error;

	/* write each key-value pair */
	for (int i = 0; i < ctx->nentries; i++)
	{
		uint64	key = ctx->keys[i];
		Jsonb	*val = ctx->values[i];
		int32	val_size = VARSIZE(val);

		/* keys are always uint64 */
		if (fwrite(&key, sizeof(uint64), 1, file) != 1)
			goto error;

		/* value size */
		if (fwrite(&(val_size), sizeof(int32), 1, file) != 1)
			goto error;

		/* value */
		if (fwrite(val, val_size, 1, file) != 1)
			goto error;
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	durable_rename(MEMSTORE_DUMP_FILE ".tmp", MEMSTORE_DUMP_FILE, LOG);

	PG_RETURN_BOOL(true);

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					MEMSTORE_DUMP_FILE ".tmp")));
	if (file)
		FreeFile(file);

	unlink(MEMSTORE_DUMP_FILE ".tmp");

	PG_RETURN_BOOL(false);
}

/*
 * Load MEMSTORE_MAGIC_NUMBER to memstore.
 */
Datum
load(PG_FUNCTION_ARGS)
{
	FILE	*file;
	int32 	nentries;
	uint32 	magic;
	char	*buffer;
	int		buffer_size;

	ensure_attach_memstore_shmem();

	file = AllocateFile(MEMSTORE_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;

		FreeFile(file);
		PG_RETURN_BOOL(true);
	}

	if (fread(&magic, sizeof(uint32), 1, file) != 1 ||
		fread(&nentries, sizeof(int32), 1, file) != 1)
		goto read_error;

	if (magic != MEMSTORE_MAGIC_NUMBER)
		goto data_error;

	buffer_size = 1024;
	buffer = palloc0(buffer_size);

	elog(NOTICE, "loading %u entries...", nentries);
	for (int i = 0; i < nentries; i++)
	{
		uint64	key;
		int32	val_size;
		Jsonb	*val;

		if (fread(&key, sizeof(uint64), 1, file) != 1)
			goto read_error;

		if (fread(&val_size, sizeof(int32), 1, file) != 1)
			goto read_error;

		if (val_size <= 0)
			goto data_error;

		if (buffer_size < val_size)
		{
			while (buffer_size < val_size)
				buffer_size *= 2;

			buffer = repalloc(buffer, buffer_size);
		}

		if (fread(buffer, val_size, 1, file) != 1)
			goto read_error;

		val = (Jsonb *) buffer;

		Assert(VARSIZE(val) >= 0);

		jbstore_set(PgMemStoreLocal.store, key, val);
	}

	pfree(buffer);
	FreeFile(file);

	PG_RETURN_BOOL(true);

read_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read file \"%s\": %m",
					MEMSTORE_DUMP_FILE)));
	goto fail;

data_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("ignoring invalid data in file \"%s\"",
					MEMSTORE_DUMP_FILE)));
	goto fail;

fail:
	if (buffer)
		pfree(buffer);
	if (file)
		FreeFile(file);

	PG_RETURN_BOOL(false);
}

/* Return the current memory usage of memstore */
Datum
memory_usage(PG_FUNCTION_ARGS)
{
	ensure_attach_memstore_shmem();

	PG_RETURN_INT64(jbstore_memory_usage(PgMemStoreLocal.store));
}

/*
 * Return all key-value pairs.
 *
 * XXX: this function can work once a bug in radixtree.h that enables
 * non-radixtree-creator can begin the iteration is fixed.
 */
Datum
list(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Datum		result;
	MemoryContext oldcontext;
	TupleDesc	tupledesc;
	MemStoreListContext *fctx;
	HeapTuple	tuple;

	ensure_attach_memstore_shmem();

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* initialize list function context */
		fctx = palloc(sizeof(MemStoreListContext));
		fctx->nentries = 0;
		fctx->max_nentries = 128;
		fctx->keys = palloc(sizeof(uint64) * fctx->max_nentries);
		fctx->values = palloc(sizeof(Jsonb *) * fctx->max_nentries);

		tupledesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "key",
						   BYTEAOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "value",
						   JSONBOID, -1, 0);
		fctx->tupdesc = BlessTupleDesc(tupledesc);

		/* collect all key-value pairs */
		collect_all_key_values(fctx);

		funcctx->max_calls = fctx->nentries;
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	fctx = funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		uint32      i = funcctx->call_cntr;
		char	buf[9] = {0};
		Datum	values[2];
		bool	nulls[2] = {false};

		memcpy(buf, &(fctx->keys[i]), sizeof(fctx->keys[i]));

		values[0] = CStringGetTextDatum(buf);
		values[1] = JsonbPGetDatum(fctx->values[i]);

		tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}

static void
collect_all_key_values(MemStoreListContext *ctx)
{
	uint64	key;
	Jsonb	*val;
	jbstore_iter	*iter;

	Assert(PgMemStoreLocal.store);

	jbstore_lock_share(PgMemStoreLocal.store);
	iter = jbstore_begin_iterate(PgMemStoreLocal.store);

	while ((val = jbstore_iterate_next(iter, &key)) != NULL)
	{
		if (ctx->nentries >= ctx->max_nentries)
		{
			ctx->max_nentries *= 2;
			ctx->keys = repalloc(ctx->keys,
								 sizeof(uint64) * ctx->max_nentries);
			ctx->values = repalloc(ctx->values,
								   sizeof(Jsonb *) * ctx->max_nentries);
		}

		ctx->keys[ctx->nentries] = key;
		ctx->values[ctx->nentries] = val;
		ctx->nentries++;
	}

	jbstore_end_iterate(iter);
	jbstore_unlock(PgMemStoreLocal.store);
}

/* Redo-related operations */

void
memstorermgrs_redo(XLogReaderState *record)
{
	xl_memstore_set *xlrec = (xl_memstore_set *) XLogRecGetData(record);
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	uint64	key;
	Jsonb	*value;

	if (info != XLOG_MEMSTORE_SET)
		elog(ERROR, "unknown op code for pg_memstore %u", info);

	ensure_attach_memstore_shmem();

	key = xlrec->key;
	value = (Jsonb *) xlrec->value;
	jbstore_set(PgMemStoreLocal.store, key, value);
}

void
memstorermgrs_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_MEMSTORE_SET)
	{
		xl_memstore_set *xlrec = (xl_memstore_set *) rec;

		appendStringInfo(buf,
						 "key: %lu, value_size: %zu",
						 xlrec->key, xlrec->val_size);
	}
}

const char *
memstorermgrs_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_MEMSTORE_SET:
			return "SET";
	}

	return NULL;
}

