## erasure code plugins

set(erasure_plugin_dir ${CMAKE_INSTALL_PKGLIBDIR}/erasure-code)

#jerasure subdir must be before shec so jerasure & neon obj libs are declared
include_directories(jerasure/jerasure/include)
include_directories(jerasure/gf-complete/include)
include_directories(jerasure)
add_subdirectory(jerasure)
add_subdirectory(lrc)
add_subdirectory(shec)

if (HAVE_BETTER_YASM_ELF64)
  add_subdirectory(isa)
  set(EC_ISA_LIB ceph_isa)
endif (HAVE_BETTER_YASM_ELF64)

add_library(erasure_code_objs OBJECT ErasureCode.cc)

add_custom_target(erasure_code_plugins DEPENDS
    ${EC_ISA_LIB}
    ceph_lrc
    ceph_jerasure
    ceph_shec
)
if(TARGET ceph_jerasure_sse3)
  add_dependencies(erasure_code_plugins ceph_jerasure_sse3)
endif()
if(TARGET ceph_jerasure_sse4)
  add_dependencies(erasure_code_plugins ceph_jerasure_sse4)
endif()
