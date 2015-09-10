## erasure code plugins

erasure_codelibdir = $(pkglibdir)/erasure-code
erasure_codelib_LTLIBRARIES =  
compressionlibdir = $(pkglibdir)/erasure-code
compressionlib_LTLIBRARIES =  

include erasure-code/jerasure/Makefile.am
include erasure-code/lrc/Makefile.am
include erasure-code/shec/Makefile.am
include erasure-code/zlib/Makefile.am

if WITH_BETTER_YASM_ELF64
include erasure-code/isa/Makefile.am
endif # WITH_BETTER_YASM_ELF64

liberasure_code_la_SOURCES = \
	erasure-code/ErasureCodePlugin.cc \
    erasure-code/Compression.cc \
    erasure-code/CompressionFake.cc \
    erasure-code/CompressionPlugin.cc
erasure-code/ErasureCodePlugin.cc: ./ceph_ver.h
erasure-code/CompressionPlugin.cc: ./ceph_ver.h
liberasure_code_la_DEPENDENCIES = $(erasure_codelib_LTLIBRARIES) \
                                  $(compressionlib_LTLIBRARIES)
if LINUX
liberasure_code_la_LIBADD = -ldl
endif # LINUX
noinst_LTLIBRARIES += liberasure_code.la

noinst_HEADERS += \
	erasure-code/ErasureCode.h \
	erasure-code/ErasureCodeInterface.h \
	erasure-code/ErasureCodePlugin.h \
    erasure-code/Compression.h \
    erasure-code/CompressionFake.h \
    erasure-code/CompressionInterface.h \
    erasure-code/CompressionPlugin.h

