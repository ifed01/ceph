# zlib plugin
noinst_HEADERS += \
  compression/zlib/zlib-1.2.8/zutil.h \
  compression/zlib/zlib-1.2.8/zlib.h \
  compression/zlib/zlib-1.2.8/zconf.h \
  compression/zlib/zlib-1.2.8/gzguts.h \
  compression/zlib/zlib-1.2.8/crc32.h \
  compression/zlib/zlib-1.2.8/inftrees.h \
  compression/zlib/zlib-1.2.8/inflate.h \
  compression/zlib/zlib-1.2.8/inffast.h \
  compression/zlib/zlib-1.2.8/inffixed.h \
  compression/zlib/zlib-1.2.8/deflate.h \
  compression/zlib/CompressionZlib.h

zlib_sources = \
  compression/Compression.cc \
  compression/zlib/zlib-1.2.8/zutil.c \
  compression/zlib/zlib-1.2.8/adler32.c \
  compression/zlib/zlib-1.2.8/compress.c \
  compression/zlib/zlib-1.2.8/crc32.c \
  compression/zlib/zlib-1.2.8/deflate.c \
  compression/zlib/zlib-1.2.8/gzclose.c \
  compression/zlib/zlib-1.2.8/gzlib.c \
  compression/zlib/zlib-1.2.8/gzread.c \
  compression/zlib/zlib-1.2.8/gzwrite.c \
  compression/zlib/zlib-1.2.8/infback.c \
  compression/zlib/zlib-1.2.8/inffast.c \
  compression/zlib/zlib-1.2.8/inflate.c \
  compression/zlib/zlib-1.2.8/inftrees.c \
  compression/zlib/zlib-1.2.8/trees.c \
  compression/zlib/zlib-1.2.8/uncompr.c \
  compression/zlib/CompressionPluginZlib.cc \
  compression/zlib/CompressionZlib.cc

compression/zlib/CompressionPluginZlib.cc: ./ceph_ver.h

libcs_zlib_la_SOURCES = ${zlib_sources}
libcs_zlib_la_CFLAGS = ${AM_CFLAGS}  \
  -I$(srcdir)/compression/zlib/zlib-1.2.8
libcs_zlib_la_CXXFLAGS= ${AM_CXXFLAGS} \
  -I$(srcdir)/compression/zlib/zlib-1.2.8
libcs_zlib_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libcs_zlib_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libcs_zlib_la_LDFLAGS += -export-symbols-regex '.*__compression_.*'
endif

compressionlib_LTLIBRARIES += libcs_zlib.la
