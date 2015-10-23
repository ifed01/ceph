# snappy plugin
noinst_HEADERS += \
  compressor/snappy/SnappyCompressor.h

snappy_sources = \
  compressor/Compressor.cc \
  compressor/snappy/CompressionPluginSnappy.cc

compressor/snappy/CompressionPluginSnappy.cc: ./ceph_ver.h

libcs_snappy_la_SOURCES = ${snappy_sources}
libcs_snappy_la_CFLAGS = ${AM_CFLAGS}  \
  -I$(srcdir)/compressor/snappy/snappy-1.2.8
libcs_snappy_la_CXXFLAGS= ${AM_CXXFLAGS} \
  -I$(srcdir)/compressor/snappy/snappy-1.2.8
libcs_snappy_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libcs_snappy_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libcs_snappy_la_LDFLAGS += -export-symbols-regex '.*__compressor_.*'
endif

compressorlib_LTLIBRARIES += libcs_snappy.la
