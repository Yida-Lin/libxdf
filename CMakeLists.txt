cmake_minimum_required(VERSION 3.7)
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.13)

project(libxdf
        VERSION 0.99.8
        DESCRIPTION "C++ library for loading multimodal, multi-rate signals stored in XDF files."
        HOMEPAGE_URL https://xdf-modules.github.io/libxdf/)

option(XDF_NO_SYSTEM_PUGIXML "Force use of bundled pugixml" OFF)

set(SOURCES
    xdf.h
    xdf.cpp
    smarc/filtering.h
    smarc/filtering.c
    smarc/multi_stage.h
    smarc/multi_stage.c
    smarc/polyfilt.h
    smarc/polyfilt.c
    smarc/remez_lp.h
    smarc/remez_lp.c
    smarc/smarc.h
    smarc/smarc.c
    smarc/stage_impl.h
    smarc/stage_impl.c
)

add_library(xdf ${SOURCES})
add_executable(loadtest test.cpp)
target_link_libraries(loadtest PRIVATE xdf)

find_package(pugixml 1.9 QUIET)
if(TARGET pugixml AND NOT XDF_NO_SYSTEM_PUGIXML)
	message(STATUS "Using system pugixml")
	target_link_libraries(xdf PRIVATE pugixml)
else()
	message(STATUS "Using bundled pugixml")
	target_sources(xdf PRIVATE pugixml/pugixml.cpp)
	target_include_directories(xdf PRIVATE pugixml)
endif()

target_compile_features(xdf PUBLIC cxx_std_20)
set_target_properties(xdf PROPERTIES OUTPUT_NAME xdf PUBLIC_HEADER xdf.h)
target_include_directories(xdf PUBLIC
	$<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>
	$<INSTALL_INTERFACE:include>
)
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
configure_package_config_file(
    ${CMAKE_CURRENT_LIST_DIR}/libxdfConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/libxdfConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)
install(TARGETS xdf
	EXPORT "${PROJECT_NAME}Targets"
	ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
	PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(EXPORT "${PROJECT_NAME}Targets"
	COMPONENT ${PROJECT_NAME}
	NAMESPACE "XDF::"
	DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libxdfConfig.cmake
    COMPONENT ${PROJECT_NAME}
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)

# set(CPACK_STRIP_FILES ON)
# set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_PACKAGE_CONTACT "yida.lin@outlook.com")
if(APPLE)
    set(CPACK_GENERATOR TBZ2)
elseif(WIN32)
    set(CPACK_GENERATOR ZIP)
elseif(UNIX)
    set(CPACK_GENERATOR DEB)
    set(CPACK_DEBIAN_LIBLSL_PACKAGE_SHLIBDEPS ON)
    set(CPACK_DEBIAN_PACKAGE_GENERATE_SHLIBS ON)
endif()
include(CPack)

