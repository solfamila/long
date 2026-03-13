cmake_minimum_required(VERSION 3.20)
project(TWSTradingGUI)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(PkgConfig REQUIRED)

pkg_check_modules(GLFW3 REQUIRED glfw3)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
find_package(Threads REQUIRED)

set(IB_API_ROOT "/opt/IBAPI" CACHE PATH "Path to Interactive Brokers API")
set(IB_API_INCLUDE_DIR "${IB_API_ROOT}/ibsapi" CACHE PATH "IB API include directory")
set(IB_API_LIB_DIR "${IB_API_ROOT}/ibsapi" CACHE PATH "IB API library directory")

set(IMGUI_DIR "${CMAKE_SOURCE_DIR}/imgui" CACHE PATH "Path to Dear ImGui")
set(IMGUI_SOURCES
    "${IMGUI_DIR}/imgui.cpp"
    "${IMGUI_DIR}/imgui_demo.cpp"
    "${IMGUI_DIR}/imgui_draw.cpp"
    "${IMGUI_DIR}/imgui_tables.cpp"
    "${IMGUI_DIR}/imgui_widgets.cpp"
    "${IMGUI_DIR}/backends/imgui_impl_glfw.cpp"
    "${IMGUI_DIR}/backends/imgui_impl_metal.mm"
    "${IMGUI_DIR}/backends/imgui_impl_metal.h"
)

if(APPLE)
    set(IMGUI_SOURCES ${IMGUI_SOURCES}
        "${IMGUI_DIR}/backends/imgui_impl_metal.h"
    )
endif()

if(WIN32)
    set(IMGUI_SOURCES ${IMGUI_SOURCES}
        "${IMGUI_DIR}/backends/imgui_impl_win32.cpp"
        "${IMGUI_DIR}/backends/imgui_impl_dx11.cpp"
    )
    find_package(D3D11 REQUIRED)
endif()

set(IXWEBSOCKET_DIR "${CMAKE_SOURCE_DIR}/ixwebsocket" CACHE PATH "Path to ixWebSocket")

set(SOURCE_FILES
    main.cpp
    app_shared.cpp
    platform_win32.cpp
    platform_metal.cpp
    trading_panel.cpp
    websocket_handlers.cpp
    dualsense_controller.cpp
    d3d_helpers.cpp
    ${IMGUI_SOURCES}
)

if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fobjc-arc")
    
    find_library(COCOA_LIBRARY Cocoa)
    find_library(METAL_LIBRARY Metal)
    find_library(QUARTZ_CORE_LIBRARY QuartzCore)
    find_library(APP_KIT_LIBRARY AppKit)
    
    set(PLATFORM_LIBS 
        ${COCOA_LIBRARY} 
        ${METAL_LIBRARY} 
        ${QUARTZ_CORE_LIBRARY}
        ${APP_KIT_LIBRARY}
    )
    
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -x objective-c++")
endif()

include_directories(
    ${CMAKE_SOURCE_DIR}
    ${GLFW3_INCLUDE_DIRS}
    ${PROTOBUF_INCLUDE_DIR}
    ${IMGUI_DIR}
    ${IMGUI_DIR}/backends
    ${IXWEBSOCKET_DIR}
)

if(EXISTS ${IB_API_INCLUDE_DIR})
    include_directories(${IB_API_INCLUDE_DIR})
endif()

add_executable(tws_gui ${SOURCE_FILES})

target_link_libraries(tws_gui PRIVATE
    ${GLFW3_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    ixwebsocket
   Threads::Threads
)

if(APPLE)
    target_link_libraries(tws_gui PRIVATE ${PLATFORM_LIBS})
endif()

if(WIN32)
    target_link_libraries(tws_gui PRIVATE d3d11 dxgi)
    target_link_libraries(tws_gui PRIVATE setupapi)
    if(EXISTS "${IB_API_LIB_DIR}/ApiLib.lib")
        target_link_libraries(tws_gui PRIVATE "${IB_API_LIB_DIR}/ApiLib.lib")
    endif()
endif()

target_compile_definitions(tws_gui PRIVATE
    $<$<BOOL:${GLFW3_LIBRARIES}>:IMGUI_HAS_GLFW>
    $<$<BOOL:${GLFW3_LIBRARIES}>:IMGUI_IMPL_GLFW_HAS_MONITOR>
)

if(APPLE)
    set_source_files_properties(platform_metal.cpp PROPERTIES COMPILE_FLAGS "-x objective-c++")
    set_source_files_properties(${IMGUI_DIR}/backends/imgui_impl_metal.mm PROPERTIES COMPILE_FLAGS "-x objective-c++")
endif()
