# zlib plugin
noinst_HEADERS += \
  compressor/zlib/CompressionZlib.h

zlib_sources = \
  compressor/Compressor.cc \
  compressor/zlib/CompressionPluginZlib.cc \
  compressor/zlib/CompressionZlib.cc

compressor/zlib/CompressionPluginZlib.cc: ./ceph_ver.h

libcs_zlib_la_SOURCES = ${zlib_sources}
libcs_zlib_la_CFLAGS = ${AM_CFLAGS}  
libcs_zlib_la_CXXFLAGS= ${AM_CXXFLAGS} 
libcs_zlib_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libcs_zlib_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libcs_zlib_la_LDFLAGS += -export-symbols-regex '.*__compressor_.*'
endif

compressorlib_LTLIBRARIES += libcs_zlib.la
