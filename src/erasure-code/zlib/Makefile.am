# zlib plugin
noinst_HEADERS += \
  erasure-code/zlib/zlib-1.2.8/zutil.h \
  erasure-code/zlib/zlib-1.2.8/zlib.h \
  erasure-code/zlib/zlib-1.2.8/zconf.h \
  erasure-code/zlib/zlib-1.2.8/gzguts.h \
  erasure-code/zlib/zlib-1.2.8/crc32.h \
  erasure-code/zlib/zlib-1.2.8/inftrees.h \
  erasure-code/zlib/zlib-1.2.8/inflate.h \
  erasure-code/zlib/zlib-1.2.8/inffast.h \
  erasure-code/zlib/zlib-1.2.8/inffixed.h \
  erasure-code/zlib/zlib-1.2.8/deflate.h \
  erasure-code/zlib/CompressionZlib.h

zlib_sources = \
  erasure-code/Compression.cc \
  erasure-code/zlib/zlib-1.2.8/zutil.c \
  erasure-code/zlib/zlib-1.2.8/adler32.c \
  erasure-code/zlib/zlib-1.2.8/compress.c \
  erasure-code/zlib/zlib-1.2.8/crc32.c \
  erasure-code/zlib/zlib-1.2.8/deflate.c \
  erasure-code/zlib/zlib-1.2.8/gzclose.c \
  erasure-code/zlib/zlib-1.2.8/gzlib.c \
  erasure-code/zlib/zlib-1.2.8/gzread.c \
  erasure-code/zlib/zlib-1.2.8/gzwrite.c \
  erasure-code/zlib/zlib-1.2.8/infback.c \
  erasure-code/zlib/zlib-1.2.8/inffast.c \
  erasure-code/zlib/zlib-1.2.8/inflate.c \
  erasure-code/zlib/zlib-1.2.8/inftrees.c \
  erasure-code/zlib/zlib-1.2.8/trees.c \
  erasure-code/zlib/zlib-1.2.8/uncompr.c \
  erasure-code/zlib/CompressionPluginZlib.cc \
  erasure-code/zlib/CompressionZlib.cc

erasure-code/zlib/CompressionPluginZlib.cc: ./ceph_ver.h

libcs_zlib_la_SOURCES = ${zlib_sources}
libcs_zlib_la_CFLAGS = ${AM_CFLAGS}  \
  -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
libcs_zlib_la_CXXFLAGS= ${AM_CXXFLAGS} \
  -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
libcs_zlib_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libcs_zlib_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
if LINUX
libcs_zlib_la_LDFLAGS += -export-symbols-regex '.*__compression_.*'
endif

compressionlib_LTLIBRARIES += libcs_zlib.la

# libcs_adler32_la_SOURCES = ${zlib_sources}
# libcs_adler32_la_CFLAGS = ${AM_CFLAGS}  \
# 	-I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_adler32_la_CXXFLAGS= ${AM_CXXFLAGS} \
# 	-I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_adler32_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
# libcs_adler32_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
# if LINUX
# libcs_adler32_la_LDFLAGS += -export-symbols-regex '.*__compression_.*'
# endif

# compressionlib_LTLIBRARIES += libcs_adler32.la

# libcs_zutil_la_SOURCES = ${zlib_sources}
# libcs_zutil_la_CFLAGS = ${AM_CFLAGS}  \
#   -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_zutil_la_CXXFLAGS= ${AM_CXXFLAGS} \
#   -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_zutil_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
# libcs_zutil_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
# if LINUX
# libcs_zutil_la_LDFLAGS += -export-symbols-regex '.*__compression_.*'
# endif

# compressionlib_LTLIBRARIES += libcs_zutil.la

# libcs_deflate_la_SOURCES = ${zlib_sources} \
#                            erasure-code/zlib/zlib-1.2.8/deflate.c
# libcs_deflate_la_CFLAGS = ${AM_CFLAGS}  \
#   -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_deflate_la_CXXFLAGS= ${AM_CXXFLAGS} \
#   -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_deflate_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
# libcs_deflate_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
# if LINUX
# libcs_deflate_la_LDFLAGS += -export-symbols-regex '.*__compression_.*'
# endif

# compressionlib_LTLIBRARIES += libcs_deflate.la


# libcs_inflate_la_SOURCES = ${zlib_sources} \
#                            erasure-code/zlib/zlib-1.2.8/inftrees.c \
#                            erasure-code/zlib/zlib-1.2.8/inflate.c \
#                            erasure-code/zlib/zlib-1.2.8/inffast.c
# libcs_inflate_la_CFLAGS = ${AM_CFLAGS}  \
#   -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_inflate_la_CXXFLAGS= ${AM_CXXFLAGS} \
#   -I$(srcdir)/erasure-code/zlib/zlib-1.2.8
# libcs_inflate_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
# libcs_inflate_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0
# if LINUX
# libcs_inflate_la_LDFLAGS += -export-symbols-regex '.*__compression_.*'
# endif

# compressionlib_LTLIBRARIES += libcs_inflate.la


