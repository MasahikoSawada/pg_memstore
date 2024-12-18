# pg_memstore

MODULE_big = pg_memstore
DATA = pg_memstore--1.0.sql
OBJS = pg_memstore.o

EXTENSION = pg_memstore
REGRESS = pg_memstore

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
