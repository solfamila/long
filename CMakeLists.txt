cmake_minimum_required(VERSION 3.20)
project(TWSTradingGUI LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(NOT APPLE)
    message(FATAL_ERROR "TWSTradingGUI now supports macOS only")
endif()

enable_language(OBJCXX)

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

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

if(NOT EXISTS "${IXWEBSOCKET_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "IXWebSocket sources not found at ${IXWEBSOCKET_DIR}")
endif()

if(NOT EXISTS "${NLOHMANN_JSON_DIR}/single_include/nlohmann/json.hpp")
    message(FATAL_ERROR "nlohmann/json headers not found at ${NLOHMANN_JSON_DIR}")
endif()

set(APP_SOURCES
    main.mm
    app_shared.cpp
    websocket_handlers.cpp
    controller_mac.mm
)

set(BUILD_DEMO OFF CACHE BOOL "" FORCE)
set(IXWEBSOCKET_INSTALL OFF CACHE BOOL "" FORCE)
set(USE_TLS OFF CACHE BOOL "" FORCE)
add_subdirectory("${IXWEBSOCKET_DIR}" EXCLUDE_FROM_ALL)

add_executable(tws_gui ${APP_SOURCES})

target_include_directories(tws_gui PRIVATE
    "${CMAKE_SOURCE_DIR}"
    "${IXWEBSOCKET_DIR}"
    "${NLOHMANN_JSON_DIR}/single_include"
)

target_link_libraries(tws_gui PRIVATE
    Threads::Threads
    ixwebsocket::ixwebsocket
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

        find_package(Protobuf REQUIRED)
        pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
        file(MAKE_DIRECTORY "${IB_API_GENERATED_PROTO_DIR}")
        file(GLOB IB_API_PROTO_FILES CONFIGURE_DEPENDS "${IB_API_PROTO_DIR}/*.proto")

        if(NOT IB_API_PROTO_FILES)
            message(FATAL_ERROR "Interactive Brokers proto sources not found at ${IB_API_PROTO_DIR}")
        endif()

        list(APPEND IB_API_LINK_LIBS PkgConfig::PROTOBUF)

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
            ${Protobuf_INCLUDE_DIRS}
            ${PROTOBUF_INCLUDE_DIRS}
        )
        list(APPEND IB_API_COMPILE_DEFINITIONS
            TWS_HAS_ORDER_CANCEL_OBJECT=1
            TWS_ERROR_HAS_TIMESTAMP=1
            TWS_HAS_COMMISSION_AND_FEES_REPORT=1
        )
    else()
        list(APPEND IB_API_LINK_LIBS "${VENDORED_IB_API_DIR}/lib/libbid.a")
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
    message(STATUS "Using vendored Interactive Brokers API sources from ${VENDORED_IB_API_DIR}")
elseif(EXISTS "${IB_API_INCLUDE_DIR}/DefaultEWrapper.h")
    target_include_directories(tws_gui PRIVATE "${IB_API_INCLUDE_DIR}")
    message(STATUS "Using Interactive Brokers API from ${IB_API_INCLUDE_DIR}")
else()
    target_include_directories(tws_gui PRIVATE "${LOCAL_IBAPI_STUB_DIR}")
    target_compile_definitions(tws_gui PRIVATE
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

set_source_files_properties(
    main.mm
    controller_mac.mm
    PROPERTIES
        LANGUAGE OBJCXX
        COMPILE_OPTIONS "-fobjc-arc"
)
