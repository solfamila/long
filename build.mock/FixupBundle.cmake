include(BundleUtilities)

set(BU_CHMOD_BUNDLE_ITEMS ON)
set(search_dirs "")

if(NOT DEFINED bundle_path OR bundle_path STREQUAL "")
    message(FATAL_ERROR "bundle_path was not provided to FixupBundle.cmake")
endif()

string(REGEX REPLACE "^\"|\"$" "" bundle_path "${bundle_path}")
string(REGEX REPLACE "^\"|\"$" "" codesign_executable "${codesign_executable}")

if(NOT EXISTS "${bundle_path}")
    message(FATAL_ERROR "Bundle path does not exist: ${bundle_path}")
endif()

set(frameworks_dir "${bundle_path}/Contents/Frameworks")
if(EXISTS "${frameworks_dir}")
    file(REMOVE_RECURSE "${frameworks_dir}")
endif()
file(MAKE_DIRECTORY "${frameworks_dir}")

fixup_bundle("${bundle_path}" "" "${search_dirs}")
verify_app("${bundle_path}")

if(DEFINED codesign_executable AND NOT codesign_executable STREQUAL "")
    execute_process(
        COMMAND "${codesign_executable}" --force --deep --sign - --timestamp=none "${bundle_path}"
        RESULT_VARIABLE codesign_result
        OUTPUT_VARIABLE codesign_output
        ERROR_VARIABLE codesign_error
    )
    if(NOT codesign_result EQUAL 0)
        message(FATAL_ERROR "codesign failed for ${bundle_path}: ${codesign_output}\n${codesign_error}")
    endif()
endif()
