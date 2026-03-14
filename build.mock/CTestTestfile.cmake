# CMake generated Testfile for 
# Source directory: /Users/foxy/intent/workspaces/short-task/repo
# Build directory: /Users/foxy/intent/workspaces/short-task/repo/build.mock
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[tws_gui_trace_tests]=] "/Users/foxy/intent/workspaces/short-task/repo/build.mock/tws_gui_tests")
set_tests_properties([=[tws_gui_trace_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/Users/foxy/intent/workspaces/short-task/repo/CMakeLists.txt;417;add_test;/Users/foxy/intent/workspaces/short-task/repo/CMakeLists.txt;0;")
subdirs("ixwebsocket")
