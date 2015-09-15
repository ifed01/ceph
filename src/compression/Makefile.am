## compression plugins

compressionlibdir = $(pkglibdir)/compression
compressionlib_LTLIBRARIES =  

include compression/zlib/Makefile.am

libcompression_la_SOURCES = \
    compression/CompressionPlugin.cc
compression/CompressionPlugin.cc: ./ceph_ver.h
libcompression_la_DEPENDENCIES = $(compressionlib_LTLIBRARIES)
if LINUX
libcompression_la_LIBADD = -ldl
endif # LINUX
noinst_LTLIBRARIES += libcompression.la

noinst_HEADERS += \
    compression/Compression.h \
    compression/CompressionInterface.h \
    compression/CompressionPlugin.h

