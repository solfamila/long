if(NOT DEFINED bundle_path OR bundle_path STREQUAL "")
    message(FATAL_ERROR "bundle_path is required")
endif()

if(NOT DEFINED zip_path OR zip_path STREQUAL "")
    message(FATAL_ERROR "zip_path is required")
endif()

if("$ENV{APPLE_NOTARY_PROFILE}" STREQUAL "")
    message(FATAL_ERROR "APPLE_NOTARY_PROFILE must name an xcrun notarytool keychain profile")
endif()

find_program(XCRUN_EXECUTABLE xcrun REQUIRED)

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E rm -f "${zip_path}"
)

execute_process(
    COMMAND ditto -c -k --norsrc --keepParent "${bundle_path}" "${zip_path}"
    RESULT_VARIABLE DITTO_RESULT
)
if(NOT DITTO_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to create notarization zip")
endif()

execute_process(
    COMMAND "${XCRUN_EXECUTABLE}" notarytool submit "${zip_path}"
            --keychain-profile "$ENV{APPLE_NOTARY_PROFILE}"
            --wait
    RESULT_VARIABLE NOTARY_RESULT
)
if(NOT NOTARY_RESULT EQUAL 0)
    message(FATAL_ERROR "Notarization failed")
endif()

execute_process(
    COMMAND "${XCRUN_EXECUTABLE}" stapler staple "${bundle_path}"
    RESULT_VARIABLE STAPLE_RESULT
)
if(NOT STAPLE_RESULT EQUAL 0)
    message(FATAL_ERROR "Stapling failed")
endif()
