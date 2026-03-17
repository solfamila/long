cmake_minimum_required(VERSION 3.20)
project(TWSTradingGUI LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(NOT CMAKE_OSX_DEPLOYMENT_TARGET)
    set(CMAKE_OSX_DEPLOYMENT_TARGET "13.0" CACHE STRING "Minimum macOS deployment target" FORCE)
endif()

option(TWS_GUI_BUILD_UNIVERSAL "Build a universal macOS binary (arm64 + x86_64)" OFF)
option(TWS_GUI_USE_STATIC_PROTOBUF "Build protobuf from source as static libs to reduce bundled dylib fan-out" ON)
set(TWS_GUI_PROTOBUF_SOURCE_DIR "" CACHE PATH "Optional local protobuf source directory for static builds")
set(TWS_GUI_PROTOBUF_URL "https://github.com/protocolbuffers/protobuf/releases/download/v34.0/protobuf-34.0.tar.gz" CACHE STRING "Official protobuf source tarball URL")
if(TWS_GUI_BUILD_UNIVERSAL)
    set(CMAKE_OSX_ARCHITECTURES "arm64;x86_64" CACHE STRING "macOS architectures" FORCE)
endif()

if(NOT APPLE)
    message(FATAL_ERROR "TWSTradingGUI now supports macOS only")
endif()

enable_language(OBJCXX)

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)
include(CTest)
include(FetchContent)
option(TWS_GUI_PREFER_HOMEBREW_CURL "Prefer Homebrew libcurl when available, including websocket-capable builds" ON)
set(TWS_GUI_HOMEBREW_CURL_PREFIXES
    "/opt/homebrew/opt/curl"
    "/usr/local/opt/curl"
)
set(TWS_GUI_SELECTED_CURL_PREFIX "")
if(TWS_GUI_PREFER_HOMEBREW_CURL)
    foreach(prefix IN LISTS TWS_GUI_HOMEBREW_CURL_PREFIXES)
        if(EXISTS "${prefix}/lib/libcurl.dylib" AND EXISTS "${prefix}/include/curl/curl.h")
            set(TWS_GUI_SELECTED_CURL_PREFIX "${prefix}")
            set(CURL_LIBRARY "${prefix}/lib/libcurl.dylib" CACHE FILEPATH "Preferred libcurl library" FORCE)
            set(CURL_INCLUDE_DIR "${prefix}/include" CACHE PATH "Preferred libcurl include directory" FORCE)
            break()
        endif()
    endforeach()
endif()
find_package(CURL REQUIRED)
if(TWS_GUI_SELECTED_CURL_PREFIX)
    message(STATUS "Using Homebrew libcurl from ${TWS_GUI_SELECTED_CURL_PREFIX}")
else()
    message(STATUS "Using CMake-discovered libcurl: ${CURL_LIBRARY}")
endif()
if(TWS_GUI_SELECTED_CURL_PREFIX)
    add_library(tws_gui_curl SHARED IMPORTED GLOBAL)
    set_target_properties(tws_gui_curl PROPERTIES
        IMPORTED_LOCATION "${TWS_GUI_SELECTED_CURL_PREFIX}/lib/libcurl.dylib"
        INTERFACE_INCLUDE_DIRECTORIES "${TWS_GUI_SELECTED_CURL_PREFIX}/include"
        INTERFACE_LINK_OPTIONS "-Wl,-rpath,${TWS_GUI_SELECTED_CURL_PREFIX}/lib"
    )
else()
    add_library(tws_gui_curl INTERFACE)
    target_link_libraries(tws_gui_curl INTERFACE CURL::libcurl)
endif()

set(APP_BUNDLE_NAME "TWS Trading GUI")
set(APP_BUNDLE_IDENTIFIER "com.foxy.twstradinggui")
set(APP_BUNDLE_VERSION "1.0.0")
if(CMAKE_OSX_ARCHITECTURES MATCHES ";")
    set(APP_PACKAGE_ARCH "universal")
else()
    set(APP_PACKAGE_ARCH "${CMAKE_SYSTEM_PROCESSOR}")
endif()
set(APP_PACKAGE_FILENAME "tws_gui-macos-${APP_PACKAGE_ARCH}.zip")
set(APP_DMG_FILENAME "tws_gui-macos-${APP_PACKAGE_ARCH}.dmg")
set(BUNDLE_SEARCH_DIRS)
if(NOT TWS_GUI_USE_STATIC_PROTOBUF)
    list(APPEND BUNDLE_SEARCH_DIRS
        "/opt/homebrew/opt/protobuf/lib"
        "/opt/homebrew/opt/abseil/lib"
    )
endif()

set(IB_API_ROOT "/opt/IBAPI" CACHE PATH "Path to Interactive Brokers API")
set(IB_API_INCLUDE_DIR "${IB_API_ROOT}/ibsapi" CACHE PATH "IB API include directory")
set(IB_API_LIB_DIR "${IB_API_ROOT}/ibsapi" CACHE PATH "IB API library directory")
set(DEFAULT_VENDORED_IB_API_DIR "${CMAKE_SOURCE_DIR}/third_party/ibapi_client_legacy")
if(EXISTS "$ENV{HOME}/Downloads/twsapi_macunix/IBJts/source/cppclient/client/DefaultEWrapper.h")
    set(DEFAULT_VENDORED_IB_API_DIR "$ENV{HOME}/Downloads/twsapi_macunix/IBJts/source/cppclient/client")
elseif(EXISTS "${CMAKE_SOURCE_DIR}/third_party/ibapi_client/DefaultEWrapper.h")
    set(DEFAULT_VENDORED_IB_API_DIR "${CMAKE_SOURCE_DIR}/third_party/ibapi_client")
endif()
set(VENDORED_IB_API_DIR "${DEFAULT_VENDORED_IB_API_DIR}" CACHE PATH "Path to vendored Interactive Brokers C++ client sources")

set(IXWEBSOCKET_DIR "${CMAKE_SOURCE_DIR}/ixwebsocket" CACHE PATH "Path to ixWebSocket")
set(NLOHMANN_JSON_DIR "${CMAKE_SOURCE_DIR}/nlohmann_json" CACHE PATH "Path to nlohmann/json")
set(LOCAL_IBAPI_STUB_DIR "${CMAKE_SOURCE_DIR}/stubs/ibapi")
set(RUNTIME_CONFIG_DIR "${CMAKE_SOURCE_DIR}/config/runtime")
set(RUNTIME_REGISTRY_INPUT "${RUNTIME_CONFIG_DIR}/queues.yaml")
set(RUNTIME_REGISTRY_GENERATED_DIR "${CMAKE_BINARY_DIR}/generated")
set(RUNTIME_REGISTRY_GENERATED_HEADER "${RUNTIME_REGISTRY_GENERATED_DIR}/runtime_registry.generated.h")

add_custom_command(
    OUTPUT "${RUNTIME_REGISTRY_GENERATED_HEADER}"
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_SOURCE_DIR}/tools/generate_runtime_registry.py"
            "${RUNTIME_REGISTRY_INPUT}"
            "${RUNTIME_REGISTRY_GENERATED_HEADER}"
    DEPENDS
            "${RUNTIME_REGISTRY_INPUT}"
            "${CMAKE_SOURCE_DIR}/tools/generate_runtime_registry.py"
    VERBATIM
)
add_custom_target(generate_runtime_registry DEPENDS "${RUNTIME_REGISTRY_GENERATED_HEADER}")

if(NOT EXISTS "${IXWEBSOCKET_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "IXWebSocket sources not found at ${IXWEBSOCKET_DIR}")
endif()

if(NOT EXISTS "${NLOHMANN_JSON_DIR}/single_include/nlohmann/json.hpp")
    message(FATAL_ERROR "nlohmann/json headers not found at ${NLOHMANN_JSON_DIR}")
endif()

set(APP_SOURCES
    main.mm
    app_shared.cpp
    controller_claim.cpp
    bridge_batch_codec.cpp
    bridge_batch_transport.cpp
    runtime_qos.cpp
    tape_engine_protocol.cpp
    mac_observability.mm
    trading_export_panels.mm
    trading_refresh_scheduler.mm
    trading_actions.cpp
    trading_settings_sheet.mm
    trading_ui_format.cpp
    trace_exporter.cpp
    trading_runtime.cpp
    trading_runtime_host.cpp
    trading_view_model.cpp
    websocket_handlers.cpp
    controller_mac.mm
)

set(TAPE_ENGINE_SOURCES
    tape_engine_main.cpp
    tape_engine.cpp
    tape_bundle_inspection.cpp
    tape_engine_client.cpp
    tape_engine_protocol.cpp
    phase3_analyzers.cpp
    bridge_batch_codec.cpp
    runtime_qos.cpp
    uw_context_cache.cpp
    uw_context_connectors.cpp
    uw_gemini_executor.cpp
    uw_http.cpp
    uw_context_normalizer.cpp
    uw_runtime.cpp
    uw_context_service.cpp
    uw_packet_builder.cpp
)

set(TAPESCOPE_SOURCES
    tapescope_app.mm
    tapescope_history.mm
    tapescope_queries.mm
    tapescope_session_queries.mm
    tapescope_order_queries.mm
    tapescope_artifact_queries.mm
    tapescope_phase7_queries.mm
    tapescope_phase7_runtime.mm
    tapescope_bundle_preview.cpp
    tapescope_client.cpp
    tape_phase7_artifacts.cpp
    tape_phase7_runtime_bridge.cpp
    tape_bundle_inspection.cpp
    tape_query_payloads.cpp
    tapescope_investigation_pane.mm
    tapescope_support_ui.mm
    tapescope_support_describe.mm
    tapescope_table_bindings.mm
    tape_engine_client.cpp
    tape_engine_protocol.cpp
    app_shared.cpp
    controller_claim.cpp
    bridge_batch_codec.cpp
    bridge_batch_transport.cpp
    runtime_qos.cpp
    websocket_handlers.cpp
    trading_runtime.cpp
    trading_runtime_host.cpp
    trace_exporter.cpp
    trading_ui_format.cpp
    controller_mac.mm
    mac_observability.mm
)

set(TAPE_MCP_SOURCES
    tape_mcp_main.cpp
    tape_mcp_adapter.cpp
    tape_mcp_engine_rpc.cpp
    tape_phase7_artifacts.cpp
    tape_query_payloads.cpp
    tape_bundle_inspection.cpp
    tape_engine_client.cpp
    tape_engine_protocol.cpp
    runtime_qos.cpp
)

set(BUILD_DEMO OFF CACHE BOOL "" FORCE)
set(IXWEBSOCKET_INSTALL OFF CACHE BOOL "" FORCE)
set(USE_TLS OFF CACHE BOOL "" FORCE)
add_subdirectory("${IXWEBSOCKET_DIR}" "${CMAKE_BINARY_DIR}/third_party/ixwebsocket" EXCLUDE_FROM_ALL)

add_executable(tws_gui MACOSX_BUNDLE ${APP_SOURCES})
add_executable(tape_engine ${TAPE_ENGINE_SOURCES})
add_executable(tape_engine_ctl
    tape_engine_ctl.cpp
    tape_engine_client.cpp
    tape_engine_protocol.cpp
)
add_executable(uw_ws_smoke
    uw_ws_smoke_main.cpp
    uw_context_connectors.cpp
    uw_http.cpp
    uw_runtime.cpp
)
add_executable(phase7_execution_smoke
    phase7_execution_smoke_main.cpp
    tape_phase7_artifacts.cpp
    tape_phase7_runtime_bridge.cpp
    tape_bundle_inspection.cpp
    tape_engine_protocol.cpp
    app_shared.cpp
    controller_claim.cpp
    bridge_batch_codec.cpp
    bridge_batch_transport.cpp
    runtime_qos.cpp
    websocket_handlers.cpp
    trading_runtime.cpp
    trading_runtime_host.cpp
    trace_exporter.cpp
    trading_ui_format.cpp
    controller_mac.mm
    mac_observability.mm
)
add_executable(TapeScope MACOSX_BUNDLE ${TAPESCOPE_SOURCES})
add_executable(tape_mcp ${TAPE_MCP_SOURCES})

target_include_directories(tws_gui PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${RUNTIME_REGISTRY_GENERATED_DIR}"
    "${IXWEBSOCKET_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

target_include_directories(tape_engine PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${RUNTIME_REGISTRY_GENERATED_DIR}"
    "${IXWEBSOCKET_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

target_include_directories(tape_engine_ctl PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${RUNTIME_REGISTRY_GENERATED_DIR}"
    "${IXWEBSOCKET_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

target_include_directories(TapeScope PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${RUNTIME_REGISTRY_GENERATED_DIR}"
    "${IXWEBSOCKET_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

target_include_directories(tape_mcp PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${RUNTIME_REGISTRY_GENERATED_DIR}"
    "${IXWEBSOCKET_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

target_include_directories(uw_ws_smoke PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

target_include_directories(phase7_execution_smoke PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${RUNTIME_REGISTRY_GENERATED_DIR}"
    "${IXWEBSOCKET_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

add_dependencies(tws_gui generate_runtime_registry)
add_dependencies(tape_engine generate_runtime_registry)
add_dependencies(tape_engine_ctl generate_runtime_registry)
add_dependencies(TapeScope generate_runtime_registry)
add_dependencies(tape_mcp generate_runtime_registry)
add_dependencies(phase7_execution_smoke generate_runtime_registry)

target_link_libraries(tws_gui PRIVATE
    Threads::Threads
    ixwebsocket::ixwebsocket
)

target_link_libraries(tape_engine PRIVATE
    Threads::Threads
    tws_gui_curl
)

target_link_libraries(tape_engine_ctl PRIVATE
    Threads::Threads
)

target_compile_definitions(uw_ws_smoke PRIVATE
    TWS_GUI_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
)
target_link_libraries(uw_ws_smoke PRIVATE
    Threads::Threads
    tws_gui_curl
)

target_link_libraries(phase7_execution_smoke PRIVATE
    Threads::Threads
    ixwebsocket::ixwebsocket
)

target_link_libraries(TapeScope PRIVATE
    Threads::Threads
    ixwebsocket::ixwebsocket
)

target_link_libraries(tape_mcp PRIVATE
    Threads::Threads
)

if(EXISTS "${VENDORED_IB_API_DIR}/DefaultEWrapper.h")
    file(GLOB IB_API_SOURCES CONFIGURE_DEPENDS "${VENDORED_IB_API_DIR}/*.cpp")
    set(IB_API_EXTRA_SOURCES)
    set(IB_API_EXTRA_INCLUDE_DIRS)
    set(IB_API_LINK_LIBS)
    set(IB_API_COMPILE_DEFINITIONS)

    if(EXISTS "${VENDORED_IB_API_DIR}/CommissionAndFeesReport.h")
        get_filename_component(IB_API_CPPCLIENT_DIR "${VENDORED_IB_API_DIR}" DIRECTORY)
        get_filename_component(IB_API_SOURCE_DIR "${IB_API_CPPCLIENT_DIR}" DIRECTORY)
        set(IB_API_PROTO_DIR "${IB_API_SOURCE_DIR}/proto")
        set(IB_API_GENERATED_PROTO_DIR "${CMAKE_BINARY_DIR}/generated/ibapi_protobuf")

        if(TWS_GUI_USE_STATIC_PROTOBUF)
            set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
            set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
            set(protobuf_BUILD_CONFORMANCE OFF CACHE BOOL "" FORCE)
            set(protobuf_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
            set(protobuf_BUILD_PROTOBUF_BINARIES ON CACHE BOOL "" FORCE)
            set(protobuf_BUILD_PROTOC_BINARIES OFF CACHE BOOL "" FORCE)
            set(protobuf_BUILD_LIBPROTOC OFF CACHE BOOL "" FORCE)
            set(protobuf_BUILD_LIBUPB OFF CACHE BOOL "" FORCE)
            set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
            set(protobuf_FORCE_FETCH_DEPENDENCIES ON CACHE BOOL "" FORCE)
            set(protobuf_LOCAL_DEPENDENCIES_ONLY OFF CACHE BOOL "" FORCE)

            if(TWS_GUI_PROTOBUF_SOURCE_DIR AND EXISTS "${TWS_GUI_PROTOBUF_SOURCE_DIR}/CMakeLists.txt")
                add_subdirectory("${TWS_GUI_PROTOBUF_SOURCE_DIR}" "${CMAKE_BINARY_DIR}/third_party/protobuf")
            else()
                FetchContent_Declare(
                    protobuf
                    URL "${TWS_GUI_PROTOBUF_URL}"
                    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
                )
                FetchContent_MakeAvailable(protobuf)
            endif()

            if(NOT TARGET protobuf::libprotobuf)
                message(FATAL_ERROR "Static protobuf build did not provide protobuf::libprotobuf")
            endif()

            list(APPEND IB_API_LINK_LIBS protobuf::libprotobuf)
        else()
            find_package(Protobuf REQUIRED)
            pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
            list(APPEND IB_API_LINK_LIBS PkgConfig::PROTOBUF)
            list(APPEND IB_API_EXTRA_INCLUDE_DIRS
                ${Protobuf_INCLUDE_DIRS}
                ${PROTOBUF_INCLUDE_DIRS}
            )
            list(APPEND BUNDLE_SEARCH_DIRS
                ${Protobuf_LIBRARY_DIRS}
                ${PROTOBUF_LIBRARY_DIRS}
            )
        endif()

        file(MAKE_DIRECTORY "${IB_API_GENERATED_PROTO_DIR}")
        file(GLOB IB_API_PROTO_FILES CONFIGURE_DEPENDS "${IB_API_PROTO_DIR}/*.proto")

        if(NOT IB_API_PROTO_FILES)
            message(FATAL_ERROR "Interactive Brokers proto sources not found at ${IB_API_PROTO_DIR}")
        endif()

        if(Protobuf_PROTOC_EXECUTABLE)
            set(IB_API_PROTOC_EXECUTABLE "${Protobuf_PROTOC_EXECUTABLE}")
        else()
            find_program(IB_API_PROTOC_EXECUTABLE protoc REQUIRED)
        endif()

        set(IB_API_GENERATED_PROTO_SRCS)
        set(IB_API_GENERATED_PROTO_HDRS)
        foreach(PROTO_FILE IN LISTS IB_API_PROTO_FILES)
            get_filename_component(PROTO_NAME "${PROTO_FILE}" NAME_WE)
            list(APPEND IB_API_GENERATED_PROTO_SRCS "${IB_API_GENERATED_PROTO_DIR}/${PROTO_NAME}.pb.cc")
            list(APPEND IB_API_GENERATED_PROTO_HDRS "${IB_API_GENERATED_PROTO_DIR}/${PROTO_NAME}.pb.h")
            add_custom_command(
                OUTPUT
                    "${IB_API_GENERATED_PROTO_DIR}/${PROTO_NAME}.pb.cc"
                    "${IB_API_GENERATED_PROTO_DIR}/${PROTO_NAME}.pb.h"
                COMMAND
                    "${IB_API_PROTOC_EXECUTABLE}"
                    "--proto_path=${IB_API_PROTO_DIR}"
                    --experimental_allow_proto3_optional
                    "--cpp_out=${IB_API_GENERATED_PROTO_DIR}"
                    "${PROTO_FILE}"
                DEPENDS "${PROTO_FILE}"
                VERBATIM
            )
        endforeach()

        add_custom_target(ibapi_proto_gen DEPENDS ${IB_API_GENERATED_PROTO_SRCS} ${IB_API_GENERATED_PROTO_HDRS})

        list(APPEND IB_API_EXTRA_SOURCES
            ${IB_API_GENERATED_PROTO_SRCS}
            "${CMAKE_SOURCE_DIR}/ib_decimal_compat.cpp"
        )
        list(APPEND IB_API_EXTRA_INCLUDE_DIRS
            "${IB_API_GENERATED_PROTO_DIR}"
        )
        list(APPEND IB_API_COMPILE_DEFINITIONS
            TWS_HAS_ORDER_CANCEL_OBJECT=1
            TWS_ERROR_HAS_TIMESTAMP=1
            TWS_HAS_COMMISSION_AND_FEES_REPORT=1
        )
    else()
        list(APPEND IB_API_EXTRA_SOURCES
            "${CMAKE_SOURCE_DIR}/ib_decimal_compat.cpp"
        )
        list(APPEND IB_API_COMPILE_DEFINITIONS
            TWS_ORDER_STATUS_PERMID_IS_INT=1
            TWS_NEEDS_DECIMAL_FUNCTIONS_SHIM=1
        )
    endif()

    add_library(twsapi STATIC ${IB_API_SOURCES} ${IB_API_EXTRA_SOURCES})
    if(TARGET ibapi_proto_gen)
        add_dependencies(twsapi ibapi_proto_gen)
    endif()
    target_include_directories(twsapi PUBLIC
        "${VENDORED_IB_API_DIR}"
        ${IB_API_EXTRA_INCLUDE_DIRS}
    )
    target_compile_definitions(twsapi PUBLIC ${IB_API_COMPILE_DEFINITIONS})
    target_link_libraries(twsapi PUBLIC ${IB_API_LINK_LIBS})
    target_compile_options(twsapi PRIVATE -Wno-switch -Wno-unused-function)

    target_include_directories(tws_gui PRIVATE
        "${VENDORED_IB_API_DIR}"
        ${IB_API_EXTRA_INCLUDE_DIRS}
    )
    target_link_libraries(tws_gui PRIVATE twsapi)
    target_include_directories(tape_engine PRIVATE
        "${VENDORED_IB_API_DIR}"
        ${IB_API_EXTRA_INCLUDE_DIRS}
    )
    target_include_directories(tape_engine_ctl PRIVATE
        "${VENDORED_IB_API_DIR}"
        ${IB_API_EXTRA_INCLUDE_DIRS}
    )
    target_include_directories(tape_mcp PRIVATE
        "${VENDORED_IB_API_DIR}"
        ${IB_API_EXTRA_INCLUDE_DIRS}
    )
    target_include_directories(phase7_execution_smoke PRIVATE
        "${VENDORED_IB_API_DIR}"
        ${IB_API_EXTRA_INCLUDE_DIRS}
    )
    target_include_directories(TapeScope PRIVATE
        "${VENDORED_IB_API_DIR}"
        ${IB_API_EXTRA_INCLUDE_DIRS}
    )
    target_compile_definitions(tape_engine PRIVATE ${IB_API_COMPILE_DEFINITIONS})
    target_compile_definitions(tape_engine_ctl PRIVATE ${IB_API_COMPILE_DEFINITIONS})
    target_compile_definitions(tape_mcp PRIVATE ${IB_API_COMPILE_DEFINITIONS})
    target_compile_definitions(phase7_execution_smoke PRIVATE ${IB_API_COMPILE_DEFINITIONS})
    target_compile_definitions(TapeScope PRIVATE ${IB_API_COMPILE_DEFINITIONS})
    target_link_libraries(phase7_execution_smoke PRIVATE twsapi)
    target_link_libraries(TapeScope PRIVATE twsapi)
    message(STATUS "Using vendored Interactive Brokers API sources from ${VENDORED_IB_API_DIR}")
elseif(EXISTS "${IB_API_INCLUDE_DIR}/DefaultEWrapper.h")
    target_include_directories(tws_gui PRIVATE "${IB_API_INCLUDE_DIR}")
    target_include_directories(tape_engine PRIVATE "${IB_API_INCLUDE_DIR}")
    target_include_directories(tape_engine_ctl PRIVATE "${IB_API_INCLUDE_DIR}")
    target_include_directories(tape_mcp PRIVATE "${IB_API_INCLUDE_DIR}")
    target_include_directories(phase7_execution_smoke PRIVATE "${IB_API_INCLUDE_DIR}")
    target_include_directories(TapeScope PRIVATE "${IB_API_INCLUDE_DIR}")
    message(STATUS "Using Interactive Brokers API from ${IB_API_INCLUDE_DIR}")
else()
    target_include_directories(tws_gui PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
    target_include_directories(tape_engine PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
    target_include_directories(tape_engine_ctl PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
    target_include_directories(phase7_execution_smoke PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
    target_include_directories(TapeScope PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
    target_include_directories(tape_mcp PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
    target_compile_definitions(tws_gui PRIVATE
        TWS_GUI_MOCK_IBAPI=1
        TWS_HAS_PROTOBUF_API=1
        TWS_HAS_ORDER_CANCEL_OBJECT=1
    )
    target_compile_definitions(tape_engine PRIVATE
        TWS_GUI_MOCK_IBAPI=1
        TWS_HAS_PROTOBUF_API=1
        TWS_HAS_ORDER_CANCEL_OBJECT=1
    )
    target_compile_definitions(tape_engine_ctl PRIVATE
        TWS_GUI_MOCK_IBAPI=1
        TWS_HAS_PROTOBUF_API=1
        TWS_HAS_ORDER_CANCEL_OBJECT=1
    )
    target_compile_definitions(phase7_execution_smoke PRIVATE
        TWS_GUI_MOCK_IBAPI=1
        TWS_HAS_PROTOBUF_API=1
        TWS_HAS_ORDER_CANCEL_OBJECT=1
    )
    target_compile_definitions(TapeScope PRIVATE
        TWS_GUI_MOCK_IBAPI=1
        TWS_HAS_PROTOBUF_API=1
        TWS_HAS_ORDER_CANCEL_OBJECT=1
    )
    target_compile_definitions(tape_mcp PRIVATE
        TWS_GUI_MOCK_IBAPI=1
        TWS_HAS_PROTOBUF_API=1
        TWS_HAS_ORDER_CANCEL_OBJECT=1
    )
    message(STATUS "Interactive Brokers API not found; building with local mock API shims")
endif()

find_library(APP_KIT_LIBRARY AppKit REQUIRED)
find_library(GAME_CONTROLLER_LIBRARY GameController REQUIRED)

target_link_libraries(tws_gui PRIVATE
    ${APP_KIT_LIBRARY}
    ${GAME_CONTROLLER_LIBRARY}
)

target_link_libraries(TapeScope PRIVATE
    ${APP_KIT_LIBRARY}
    ${GAME_CONTROLLER_LIBRARY}
)

target_link_libraries(phase7_execution_smoke PRIVATE
    ${APP_KIT_LIBRARY}
    ${GAME_CONTROLLER_LIBRARY}
)

set_target_properties(tws_gui PROPERTIES
    OUTPUT_NAME "tws_gui"
    MACOSX_BUNDLE_BUNDLE_NAME "${APP_BUNDLE_NAME}"
    MACOSX_BUNDLE_GUI_IDENTIFIER "${APP_BUNDLE_IDENTIFIER}"
    MACOSX_BUNDLE_BUNDLE_VERSION "${APP_BUNDLE_VERSION}"
    MACOSX_BUNDLE_SHORT_VERSION_STRING "${APP_BUNDLE_VERSION}"
    MACOSX_BUNDLE_INFO_PLIST "${CMAKE_SOURCE_DIR}/cmake/Info.plist.in"
    BUILD_RPATH "@executable_path/../Frameworks"
    INSTALL_RPATH "@executable_path/../Frameworks"
)

set_target_properties(TapeScope PROPERTIES
    OUTPUT_NAME "TapeScope"
    MACOSX_BUNDLE_BUNDLE_NAME "TapeScope"
    MACOSX_BUNDLE_GUI_IDENTIFIER "com.foxy.tapescope"
    MACOSX_BUNDLE_BUNDLE_VERSION "${APP_BUNDLE_VERSION}"
    MACOSX_BUNDLE_SHORT_VERSION_STRING "${APP_BUNDLE_VERSION}"
    MACOSX_BUNDLE_INFO_PLIST "${CMAKE_SOURCE_DIR}/cmake/Info.plist.in"
)

find_program(CODESIGN_EXECUTABLE codesign)
configure_file("${CMAKE_SOURCE_DIR}/cmake/FixupBundle.cmake.in"
               "${CMAKE_BINARY_DIR}/FixupBundle.cmake"
               @ONLY)
configure_file("${CMAKE_SOURCE_DIR}/cmake/NotarizeBundle.cmake.in"
               "${CMAKE_BINARY_DIR}/NotarizeBundle.cmake"
               @ONLY)

add_custom_command(TARGET tws_gui POST_BUILD
    COMMAND "${CMAKE_COMMAND}"
            -Dbundle_path="$<TARGET_BUNDLE_DIR:tws_gui>"
            -Dcodesign_executable="${CODESIGN_EXECUTABLE}"
            -P "${CMAKE_BINARY_DIR}/FixupBundle.cmake"
    VERBATIM
)

add_custom_target(tws_gui_zip
    COMMAND "${CMAKE_COMMAND}" -E rm -f "${CMAKE_BINARY_DIR}/${APP_PACKAGE_FILENAME}"
    COMMAND ditto -c -k --norsrc --keepParent
            "$<TARGET_BUNDLE_DIR:tws_gui>"
            "${CMAKE_BINARY_DIR}/${APP_PACKAGE_FILENAME}"
    DEPENDS tws_gui
    VERBATIM
)

set(APP_DMG_STAGING_DIR "${CMAKE_BINARY_DIR}/dmg-root")
add_custom_target(tws_gui_dmg
    COMMAND "${CMAKE_COMMAND}" -E rm -rf "${APP_DMG_STAGING_DIR}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${APP_DMG_STAGING_DIR}"
    COMMAND "${CMAKE_COMMAND}" -E copy_directory
            "$<TARGET_BUNDLE_DIR:tws_gui>"
            "${APP_DMG_STAGING_DIR}/tws_gui.app"
    COMMAND ln -sfn /Applications "${APP_DMG_STAGING_DIR}/Applications"
    COMMAND hdiutil create
            -volname "${APP_BUNDLE_NAME}"
            -srcfolder "${APP_DMG_STAGING_DIR}"
            -ov
            -format UDZO
            "${CMAKE_BINARY_DIR}/${APP_DMG_FILENAME}"
    DEPENDS tws_gui
    VERBATIM
)

add_custom_target(tws_gui_notarize
    COMMAND "${CMAKE_COMMAND}"
            -Dbundle_path="$<TARGET_BUNDLE_DIR:tws_gui>"
            -Dzip_path="${CMAKE_BINARY_DIR}/${APP_BUNDLE_NAME}-notarize.zip"
            -P "${CMAKE_BINARY_DIR}/NotarizeBundle.cmake"
    DEPENDS tws_gui
    VERBATIM
)

if(BUILD_TESTING)
    add_executable(tws_gui_tests
        tests/trade_trace_tests.cpp
        app_shared.cpp
        controller_claim.cpp
        bridge_batch_codec.cpp
        bridge_batch_transport.cpp
        phase3_analyzers.cpp
        runtime_qos.cpp
        websocket_handlers.cpp
        tape_phase7_artifacts.cpp
        tape_bundle_inspection.cpp
        tape_engine.cpp
        tape_engine_client.cpp
        tape_engine_protocol.cpp
        uw_context_cache.cpp
        uw_context_connectors.cpp
        uw_gemini_executor.cpp
        uw_http.cpp
        uw_context_normalizer.cpp
        uw_runtime.cpp
        uw_context_service.cpp
        uw_packet_builder.cpp
        tapescope_bundle_preview.cpp
        tapescope_client.cpp
        tape_query_payloads.cpp
        tape_phase7_runtime_bridge.cpp
        trace_exporter.cpp
        controller_mac.mm
        trading_runtime.cpp
        trading_ui_format.cpp
        mac_observability.mm
    )

    target_include_directories(tws_gui_tests PRIVATE
        "${CMAKE_SOURCE_DIR}"
        "${RUNTIME_REGISTRY_GENERATED_DIR}"
        "${IXWEBSOCKET_DIR}"
        "${NLOHMANN_JSON_DIR}/single_include"
    )
    add_dependencies(tws_gui_tests generate_runtime_registry)
    target_compile_definitions(tws_gui_tests PRIVATE
        TWS_GUI_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
    )

    target_link_libraries(tws_gui_tests PRIVATE
        Threads::Threads
        tws_gui_curl
        ixwebsocket::ixwebsocket
        ${APP_KIT_LIBRARY}
        ${GAME_CONTROLLER_LIBRARY}
    )

    if(TARGET twsapi)
        target_link_libraries(tws_gui_tests PRIVATE twsapi)
        target_include_directories(tws_gui_tests PRIVATE
            "${VENDORED_IB_API_DIR}"
            ${IB_API_EXTRA_INCLUDE_DIRS}
        )
    elseif(EXISTS "${IB_API_INCLUDE_DIR}/DefaultEWrapper.h")
        target_include_directories(tws_gui_tests PRIVATE "${IB_API_INCLUDE_DIR}")
    else()
        target_include_directories(tws_gui_tests PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
        target_compile_definitions(tws_gui_tests PRIVATE
            TWS_GUI_MOCK_IBAPI=1
            TWS_HAS_PROTOBUF_API=1
            TWS_HAS_ORDER_CANCEL_OBJECT=1
        )
    endif()

    add_test(NAME tws_gui_trace_tests COMMAND tws_gui_tests)

    add_executable(tape_mcp_contract_tests
        tests/tape_mcp_contract_tests.cpp
        app_shared.cpp
        controller_claim.cpp
        tape_mcp_adapter.cpp
        tape_mcp_engine_rpc.cpp
        tape_phase7_artifacts.cpp
        tape_phase7_runtime_bridge.cpp
        tape_query_payloads.cpp
        tape_bundle_inspection.cpp
        tape_engine.cpp
        tape_engine_client.cpp
        tape_engine_protocol.cpp
        uw_context_cache.cpp
        uw_context_connectors.cpp
        uw_gemini_executor.cpp
        uw_http.cpp
        uw_context_normalizer.cpp
        uw_runtime.cpp
        uw_context_service.cpp
        uw_packet_builder.cpp
        phase3_analyzers.cpp
        bridge_batch_codec.cpp
        bridge_batch_transport.cpp
        runtime_qos.cpp
        websocket_handlers.cpp
        trace_exporter.cpp
        controller_mac.mm
        trading_runtime.cpp
        trading_ui_format.cpp
        mac_observability.mm
    )

    target_include_directories(tape_mcp_contract_tests PRIVATE
        "${CMAKE_SOURCE_DIR}"
        "${RUNTIME_REGISTRY_GENERATED_DIR}"
        "${IXWEBSOCKET_DIR}"
        "${NLOHMANN_JSON_DIR}/single_include"
    )
    add_dependencies(tape_mcp_contract_tests generate_runtime_registry tape_mcp)
    target_compile_definitions(tape_mcp_contract_tests PRIVATE
        TWS_GUI_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
        TAPE_MCP_EXECUTABLE_PATH="$<TARGET_FILE:tape_mcp>"
    )
    target_link_libraries(tape_mcp_contract_tests PRIVATE
        Threads::Threads
        tws_gui_curl
        ixwebsocket::ixwebsocket
        ${APP_KIT_LIBRARY}
        ${GAME_CONTROLLER_LIBRARY}
    )

    if(TARGET twsapi)
        target_link_libraries(tape_mcp_contract_tests PRIVATE twsapi)
        target_include_directories(tape_mcp_contract_tests PRIVATE
            "${VENDORED_IB_API_DIR}"
            ${IB_API_EXTRA_INCLUDE_DIRS}
        )
    elseif(EXISTS "${IB_API_INCLUDE_DIR}/DefaultEWrapper.h")
        target_include_directories(tape_mcp_contract_tests PRIVATE "${IB_API_INCLUDE_DIR}")
    else()
        target_include_directories(tape_mcp_contract_tests PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
        target_compile_definitions(tape_mcp_contract_tests PRIVATE
            TWS_GUI_MOCK_IBAPI=1
            TWS_HAS_PROTOBUF_API=1
            TWS_HAS_ORDER_CANCEL_OBJECT=1
        )
    endif()

    add_test(NAME tape_mcp_contract_tests COMMAND tape_mcp_contract_tests)
endif()

set_source_files_properties(
    main.mm
    mac_observability.mm
    tapescope_app.mm
    tapescope_history.mm
    tapescope_queries.mm
    tapescope_session_queries.mm
    tapescope_order_queries.mm
    tapescope_artifact_queries.mm
    tapescope_phase7_queries.mm
    tapescope_investigation_pane.mm
    tapescope_support_ui.mm
    tapescope_support_describe.mm
    tapescope_table_bindings.mm
    trading_export_panels.mm
    trading_refresh_scheduler.mm
    trading_settings_sheet.mm
    controller_mac.mm
    PROPERTIES
        LANGUAGE OBJCXX
        COMPILE_OPTIONS "-fobjc-arc"
)
