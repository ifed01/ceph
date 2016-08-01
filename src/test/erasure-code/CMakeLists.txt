
add_executable(ceph_erasure_code_benchmark 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  ceph_erasure_code_benchmark.cc)
target_link_libraries(ceph_erasure_code_benchmark osd common ${Boost_PROGRAM_OPTIONS_LIBRARY} global ${CMAKE_DL_LIBS})
install(TARGETS ceph_erasure_code_benchmark
  DESTINATION bin)

add_executable(ceph_erasure_code_non_regression ceph_erasure_code_non_regression.cc)
target_link_libraries(ceph_erasure_code_non_regression osd common ${Boost_PROGRAM_OPTIONS_LIBRARY} global ${CMAKE_DL_LIBS})

add_executable(ceph_erasure_code ceph_erasure_code.cc)
target_link_libraries(ceph_erasure_code osd common ${Boost_PROGRAM_OPTIONS_LIBRARY} global ${CMAKE_DL_LIBS})
install(TARGETS ceph_erasure_code
  DESTINATION bin)

add_library(ceph_erasurecode_example SHARED 
  ErasureCodePluginExample.cc
  $<TARGET_OBJECTS:erasure_code_objs>)
add_dependencies(ceph_erasurecode_example ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_erasurecode_example pthread ${EXTRALIBS})

add_library(ceph_missing_entry_point SHARED ErasureCodePluginMissingEntryPoint.cc)
add_dependencies(ceph_missing_entry_point ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_missing_entry_point pthread ${EXTRALIBS})

add_library(ceph_missing_version SHARED ErasureCodePluginMissingVersion.cc)
add_dependencies(ceph_missing_version ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_missing_version pthread ${EXTRALIBS})

add_library(ceph_hangs SHARED ErasureCodePluginHangs.cc)
add_dependencies(ceph_hangs ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_hangs pthread ${EXTRALIBS})

add_library(ceph_fail_to_initialize SHARED ErasureCodePluginFailToInitialize.cc)
add_dependencies(ceph_fail_to_initialize ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_fail_to_initialize pthread ${EXTRALIBS})

add_library(ceph_fail_to_register SHARED ErasureCodePluginFailToRegister.cc)
add_dependencies(ceph_fail_to_register ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_fail_to_register pthread ${EXTRALIBS})

# unittest_erasure_code_plugin
add_executable(unittest_erasure_code_plugin
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePlugin.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin)
target_link_libraries(unittest_erasure_code_plugin
  global
  osd
  ${CMAKE_DL_LIBS}
  ceph_erasurecode_example
  common
  )
add_dependencies(unittest_erasure_code_plugin
  ceph_erasurecode_example
  ceph_missing_entry_point
  ceph_missing_version
  ceph_hangs
  ceph_fail_to_initialize
  ceph_fail_to_register)

# unittest_erasure_code
add_executable(unittest_erasure_code
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCode.cc
  )
add_ceph_unittest(unittest_erasure_code ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code)
target_link_libraries(unittest_erasure_code
  global
  osd
  common
  )

add_library(ceph_test_jerasure_neon SHARED TestJerasurePluginNEON.cc)
add_dependencies(ceph_test_jerasure_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_jerasure_neon pthread ${EXTRALIBS})

add_library(ceph_test_jerasure_sse4 SHARED TestJerasurePluginSSE4.cc)
add_dependencies(ceph_test_jerasure_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_jerasure_sse4 pthread ${EXTRALIBS})

add_library(ceph_test_jerasure_sse3 SHARED TestJerasurePluginSSE3.cc)
add_dependencies(ceph_test_jerasure_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_jerasure_sse3 pthread ${EXTRALIBS})

add_library(ceph_test_jerasure_generic SHARED TestJerasurePluginGeneric.cc)
add_dependencies(ceph_test_jerasure_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_jerasure_generic pthread ${EXTRALIBS})

# unittest_erasure_code_plugin_jerasure
add_executable(unittest_erasure_code_plugin_jerasure
  TestErasureCodePluginJerasure.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_jerasure ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_jerasure)
target_link_libraries(unittest_erasure_code_plugin_jerasure
  global
  osd
  ceph_jerasure_generic
  common)
add_dependencies(unittest_erasure_code_plugin_jerasure
  ceph_jerasure
  ceph_jerasure_sse3
  ceph_jerasure_sse4
  ceph_test_jerasure_neon 
  ceph_test_jerasure_sse4
  ceph_test_jerasure_sse3
  ceph_test_jerasure_generic)

if(HAVE_BETTER_YASM_ELF64)

#unittest_erasure_code_isa
add_executable(unittest_erasure_code_isa
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeIsa.cc
  )
add_ceph_unittest(unittest_erasure_code_isa ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_isa)
target_link_libraries(unittest_erasure_code_isa
  global
  osd
  common
  ceph_isa
  )

#unittest_erasure_code_plugin_isa
add_executable(unittest_erasure_code_plugin_isa
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodePluginIsa.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_isa ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_isa)
target_link_libraries(unittest_erasure_code_plugin_isa
  global
  osd
  common
  ceph_isa
  crush
  ${CMAKE_DL_LIBS}
  )
endif(HAVE_BETTER_YASM_ELF64)

# unittest_erasure_code_lrc
add_executable(unittest_erasure_code_lrc
  TestErasureCodeLrc.cc)
add_ceph_unittest(unittest_erasure_code_lrc ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_lrc)
target_link_libraries(unittest_erasure_code_lrc
  global
  osd
  ${CMAKE_DL_LIBS}
  ceph_lrc
  common
  )

# unittest_erasure_code_plugin_lrc
add_executable(unittest_erasure_code_plugin_lrc
  TestErasureCodePluginLrc.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_lrc ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_lrc)
add_dependencies(unittest_erasure_code_plugin_lrc 
  ceph_lrc
  ceph_jerasure
  ceph_jerasure_sse3
  ceph_jerasure_sse4
  ceph_jerasure_generic)
target_link_libraries(unittest_erasure_code_plugin_lrc
  global
  osd
  ${CMAKE_DL_LIBS}
  ceph_lrc
  ceph_jerasure_generic
  common)

add_library(ceph_test_shec_neon SHARED TestShecPluginNEON.cc)
add_dependencies(ceph_test_shec_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_shec_neon pthread ${EXTRALIBS})

add_library(ceph_test_shec_sse4 SHARED TestShecPluginSSE4.cc)
add_dependencies(ceph_test_shec_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_shec_sse4 pthread ${EXTRALIBS})

add_library(ceph_test_shec_sse3 SHARED TestShecPluginSSE3.cc)
add_dependencies(ceph_test_shec_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_shec_sse3 pthread ${EXTRALIBS})

add_library(ceph_test_shec_generic SHARED TestShecPluginGeneric.cc)
add_dependencies(ceph_test_shec_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_test_shec_generic pthread ${EXTRALIBS})

# unittest_erasure_code_plugin_shec
add_executable(unittest_erasure_code_plugin_shec
  TestErasureCodePluginShec.cc
  )
add_ceph_unittest(unittest_erasure_code_plugin_shec ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_plugin_shec)
target_link_libraries(unittest_erasure_code_plugin_shec
  global
  osd
  ${CMAKE_DL_LIBS}
  common
  ceph_shec_generic)
add_dependencies(unittest_erasure_code_plugin_shec 
  ceph_shec
  ceph_shec_sse3
  ceph_shec_sse4
  #ec_shec_neon
  ceph_test_shec_neon
  ceph_test_shec_sse3
  ceph_test_shec_sse4
  ceph_test_shec_generic)

# unittest_erasure_code_example
add_executable(unittest_erasure_code_example
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc
  TestErasureCodeExample.cc
)
add_ceph_unittest(unittest_erasure_code_example ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_example)
target_link_libraries(unittest_erasure_code_example
  global
  osd
  ${CMAKE_DL_LIBS}
  common
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_erasure_code_example PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/include)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code//jerasure/gf-complete/include)

# unittest_erasure_code_jerasure
add_executable(unittest_erasure_code_jerasure
  TestErasureCodeJerasure.cc
  )
add_ceph_unittest(unittest_erasure_code_jerasure ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_jerasure)
target_link_libraries(unittest_erasure_code_jerasure
  global
  osd
  common
  ceph_jerasure_generic
  )

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/shec)

# unittest_erasure_code_shec
add_executable(unittest_erasure_code_shec
  TestErasureCodeShec.cc
  )
add_ceph_unittest(unittest_erasure_code_shec ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec)
target_link_libraries(unittest_erasure_code_shec
  global
  osd
  ${CMAKE_DL_LIBS}
  common
  ceph_shec_generic
  )

# unittest_erasure_code_shec_all
add_executable(unittest_erasure_code_shec_all
  TestErasureCodeShec_all.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_all ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_all)
target_link_libraries(unittest_erasure_code_shec_all
  global
  osd
  ${CMAKE_DL_LIBS}
  common
  ceph_shec_generic
  )

# unittest_erasure_code_shec_thread
add_executable(unittest_erasure_code_shec_thread
  TestErasureCodeShec_thread.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_thread ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_thread)
target_link_libraries(unittest_erasure_code_shec_thread
  global
  osd
  ${CMAKE_DL_LIBS}
  common
  ceph_shec_generic
  )


# unittest_erasure_code_shec_arguments
add_executable(unittest_erasure_code_shec_arguments
  TestErasureCodeShec_arguments.cc
  )
add_ceph_unittest(unittest_erasure_code_shec_arguments ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_erasure_code_shec_arguments)
target_link_libraries(unittest_erasure_code_shec_arguments
  global
  osd
  ${CMAKE_DL_LIBS}
  common
  ceph_shec_generic
  )
