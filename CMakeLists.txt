cmake_minimum_required(VERSION 3.20)
project(TWSTradingGUI)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(PkgConfig REQUIRED)

if(APPLE)
    set(USE_NATIVE_APPLE_SHELL TRUE CACHE BOOL "Use native AppKit + MTKView shell")
endif()

if(NOT USE_NATIVE_APPLE_SHELL)
    pkg_check_modules(GLFW3 REQUIRED glfw3)
endif()

pkg_check_modules(PROTOBUF REQUIRED protobuf)
pkg_check_modules(NLOHMANN_JSON REQUIRED nlohmann_json)
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
    "${IMGUI_DIR}/misc/cpp/imgui_stdlib.cpp"
    "${IMGUI_DIR}/backends/imgui_impl_metal.mm"
    "${IMGUI_DIR}/backends/imgui_impl_metal.h"
)

if(APPLE AND USE_NATIVE_APPLE_SHELL)
    set(IMGUI_SOURCES ${IMGUI_SOURCES}
        "${IMGUI_DIR}/backends/imgui_impl_osx.mm"
        "${IMGUI_DIR}/backends/imgui_impl_osx.h"
    )
else()
    set(IMGUI_SOURCES ${IMGUI_SOURCES}
        "${IMGUI_DIR}/backends/imgui_impl_glfw.cpp"
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

add_subdirectory(${IXWEBSOCKET_DIR} ixwebsocket_build)

set(SOURCE_FILES
    app_shared.cpp
    trading_panel.cpp
    trading_runtime.cpp
    websocket_handlers.cpp
    ${IMGUI_SOURCES}
)

if(APPLE)
    set(SOURCE_FILES ${SOURCE_FILES}
        controller_mac.mm
    )
endif()

if(APPLE AND USE_NATIVE_APPLE_SHELL)
    set(SOURCE_FILES ${SOURCE_FILES}
        main.mm
    )
else()
    set(SOURCE_FILES ${SOURCE_FILES}
        main.cpp
        platform_metal.mm
    )
endif()

if(WIN32)
    set(SOURCE_FILES ${SOURCE_FILES}
        platform_win32.cpp
        d3d_helpers.cpp
        dualsense_controller.cpp
    )
endif()

if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fobjc-arc")
    
    find_library(COCOA_LIBRARY Cocoa)
    find_library(METAL_LIBRARY Metal)
    find_library(METALKIT_LIBRARY MetalKit)
    find_library(QUARTZ_CORE_LIBRARY QuartzCore)
    find_library(APP_KIT_LIBRARY AppKit)
    find_library(GAME_CONTROLLER_LIBRARY GameController)
    
    if(USE_NATIVE_APPLE_SHELL)
        set(PLATFORM_LIBS 
            ${COCOA_LIBRARY} 
            ${METAL_LIBRARY} 
            ${METALKIT_LIBRARY}
            ${QUARTZ_CORE_LIBRARY}
            ${APP_KIT_LIBRARY}
            ${GAME_CONTROLLER_LIBRARY}
        )
    else()
        set(PLATFORM_LIBS 
            ${COCOA_LIBRARY} 
            ${METAL_LIBRARY} 
            ${QUARTZ_CORE_LIBRARY}
            ${APP_KIT_LIBRARY}
            ${GAME_CONTROLLER_LIBRARY}
        )
    endif()
    
    if(NOT USE_NATIVE_APPLE_SHELL)
        set_source_files_properties(platform_metal.mm PROPERTIES COMPILE_FLAGS "-x objective-c++")
    endif()
    
    set_source_files_properties(controller_mac.mm PROPERTIES COMPILE_FLAGS "-x objective-c++")
    set_source_files_properties(main.mm PROPERTIES COMPILE_FLAGS "-x objective-c++")
    set_source_files_properties(${IMGUI_DIR}/backends/imgui_impl_osx.mm PROPERTIES COMPILE_FLAGS "-x objective-c++")
endif()

include_directories(
    ${CMAKE_SOURCE_DIR}
    ${PROTOBUF_INCLUDE_DIR}
    ${NLOHMANN_JSON_INCLUDE_DIRS}
    ${IMGUI_DIR}
    ${IMGUI_DIR}/backends
    ${IMGUI_DIR}/misc/cpp
    ${IXWEBSOCKET_DIR}
)

if(NOT USE_NATIVE_APPLE_SHELL)
    include_directories(${GLFW3_INCLUDE_DIRS})
endif()

if(EXISTS ${IB_API_INCLUDE_DIR})
    include_directories(${IB_API_INCLUDE_DIR})
    add_compile_definitions(IB_API_AVAILABLE)
else()
    add_compile_definitions(IB_API_STUB)
endif()

add_executable(tws_gui ${SOURCE_FILES})

set(TARGET_LIBS
    ${PROTOBUF_LIBRARIES}
    ${NLOHMANN_JSON_LIBRARIES}
    ixwebsocket
    Threads::Threads
)

if(NOT USE_NATIVE_APPLE_SHELL)
    set(TARGET_LIBS ${TARGET_LIBS} ${GLFW3_LIBRARIES})
endif()

target_link_libraries(tws_gui PRIVATE ${TARGET_LIBS})

if(APPLE)
    target_link_libraries(tws_gui PRIVATE ${PLATFORM_LIBS})
    target_link_directories(tws_gui PRIVATE /opt/homebrew/Cellar/protobuf/34.0/lib /opt/homebrew/Cellar/abseil/20260107.1/lib)
    if(NOT USE_NATIVE_APPLE_SHELL)
        target_link_directories(tws_gui PRIVATE /opt/homebrew/Cellar/glfw/3.4/lib)
    endif()
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
    $<$<BOOL:${GLFW3_LIBRARIES}>:GLFW_EXPOSE_NATIVE_COCOA>
)

if(APPLE)
    set_source_files_properties(${IMGUI_DIR}/backends/imgui_impl_metal.mm PROPERTIES COMPILE_FLAGS "-x objective-c++")
endif()
