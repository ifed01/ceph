# MaxLinear accelerator

set(mxl_sources
  CompressionPluginMxl.cc
  MxlCompressor.cc)

add_library(MXL::MXL SHARED IMPORTED)
set_target_properties(MXL::MXL PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES ${MXL_PANTHER_SDK_PATH}/sdk/h
  IMPORTED_LOCATION ${MXL_PANTHER_SDK_PATH}/libpan3.so)

add_library(ceph_mxl SHARED ${mxl_sources})
target_link_libraries(ceph_mxl PRIVATE MXL::MXL $<$<PLATFORM_ID:Windows>:ceph-common>)
set_target_properties(ceph_mxl PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_mxl DESTINATION ${compressor_plugin_dir})
