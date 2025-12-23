EXTENSION = pg_index_reclaim
DATA = pg_index_reclaim--1.0.sql

MODULE_big = pg_index_reclaim
OBJS = \
	pg_index_reclaim.o \
	$(WIN32RES)

PG_CPPFLAGS = -I$(top_srcdir)/src/include
SHLIB_LINK = $(filter -lm, $(LIBS))

REGRESS = pg_index_reclaim

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_index_reclaim
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

