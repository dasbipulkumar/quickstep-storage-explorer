# This file copyright (c) 2011-2013, the Quickstep authors.
# See file CREDITS.txt for details.
#
# This file is part of Quickstep.
#
# Quickstep is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Quickstep is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Quickstep.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 2.8.6)
project (QUICKSTEP)

# CMake mistakes Open64 for GCC, so see which compiler we're actually working with here.
if(CMAKE_COMPILER_IS_GNUCXX)
  exec_program(${CMAKE_CXX_COMPILER} ARGS --version OUTPUT_VARIABLE _gcc_version_str)
  string(REGEX MATCH "Open64" O64_FIND "${_gcc_version_str}")
  if(O64_FIND)
    set(COMPILER_ACTUALLY_OPEN64 TRUE)
  else()
    set(COMPILER_ACTUALLY_OPEN64 FALSE)
  endif()
endif()

if (WIN32)
  # Make Windows compilers shut up about using standard libc functions.
  set_property(
    DIRECTORY
    APPEND PROPERTY COMPILE_DEFINITIONS _CRT_SECURE_NO_WARNINGS
  )
endif()

if (${CMAKE_SYSTEM_NAME} MATCHES "NetBSD"
    OR ${CMAKE_SYSTEM_NAME} MATCHES "Haiku"
    OR CYGWIN
    OR (NOT UNIX)
    OR ${CMAKE_CXX_COMPILER_ID} MATCHES "SunPro"
    OR COMPILER_ACTUALLY_OPEN64)
  option(USE_TCMALLOC "Use tcmalloc (Google's thread-cacheing malloc) instead of system-provided malloc" OFF)
else()
  option(USE_TCMALLOC "Use tcmalloc (Google's thread-cacheing malloc) instead of system-provided malloc" ON)
endif()

# Move to third_party for release branch.
option(USE_INTEL_PCM "Enable cache-miss measurements using Intel PCM library (requires root)" ON)

# Turn on the QUICKSTEP_DEBUG flag in the source if this is a debug build.
set_property(
  DIRECTORY
  APPEND PROPERTY COMPILE_DEFINITIONS_DEBUG QUICKSTEP_DEBUG
)

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)

# Check for 64-bit build flags.
set(CMAKE_REQUIRED_LIBRARIES "-m64")
CHECK_C_COMPILER_FLAG("-m64" C_COMPILER_HAS_M64)
CHECK_CXX_COMPILER_FLAG("-m64" CXX_COMPILER_HAS_M64)
unset(CMAKE_REQUIRED_LIBRARIES)

if(C_COMPILER_HAS_M64 AND CXX_COMPILER_HAS_M64)
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -m64")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m64")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m64")
elseif(C_COMPILER_HAS_M64 OR CXX_COMPILER_HAS_M64)
  message(FATAL_ERROR "Either your C or C++ compiler supports the -m64 flag for 64-bit builds, "
                      "but not both. Make sure your compilers are from the same source.")
endif()

# Link against the system's threading library.
find_package(Threads REQUIRED)
set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT})

# Try to find C99 stdint types.
include(CheckTypeSize)
CHECK_TYPE_SIZE("int8_t" INT8_T)
CHECK_TYPE_SIZE("uint8_t" UINT8_T)
CHECK_TYPE_SIZE("int16_t" INT16_T)
CHECK_TYPE_SIZE("uint16_t" UINT16_T)
CHECK_TYPE_SIZE("int32_t" INT32_T)
CHECK_TYPE_SIZE("uint32_t" UINT32_T)
CHECK_TYPE_SIZE("int64_t" INT64_T)
CHECK_TYPE_SIZE("uint64_t" UINT64_T)
if(NOT (HAVE_INT8_T AND HAVE_UINT8_T AND HAVE_INT16_T AND HAVE_UINT16_T
        AND HAVE_INT32_T AND HAVE_UINT32_T AND HAVE_INT64_T AND HAVE_UINT64_T))
  message(FATAL_ERROR "Your system is missing some of the C99 stdint types. Does your compiler "
                      "support at least C99?")
endif()

# Check the size of "long" (normally we just use int64_t, but we need to know
# whether to use "long" or "long long" versions of some library functions on
# 64-bit integers).
CHECK_TYPE_SIZE("long" LONG BUILTIN_TYPES_ONLY)
if (LONG EQUAL 8)
  set(QUICKSTEP_LONG_64BIT TRUE)
else()
  set(QUICKSTEP_LONG_64BIT FALSE)
endif()

# Check the size of size_t. The utility module (specifically the hash function
# for pairs) needs this.
check_type_size("size_t" SIZE_T)
if(SIZE_T EQUAL 8)
  set(QUICKSTEP_SIZE_T_64BIT TRUE)
else()
  set(QUICKSTEP_SIZE_T_64BIT FALSE)
endif()

# Enable C++11 support if possible.
set(CPP11_SUPPORTED FALSE)
include(CheckCXXSourceCompiles)
if(NOT CPP11_SUPPORTED)
  if (MSVC_VERSION GREATER 1599)
    message("Your C++ compiler appears to be Microsoft Visual Studio 10 or better, which supports C++11 by default.")
    set(CPP11_SUPPORTED TRUE)
  endif()
endif()
if (NOT CPP11_SUPPORTED)
  CHECK_CXX_COMPILER_FLAG("-std=c++11" CXX_COMPILER_SUPPORTS_CPP11)
  if (CXX_COMPILER_SUPPORTS_CPP11)
    set(ORIGINAL_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    set(ORIGINAL_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}")
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS_CXX} -std=c++11")
    # This test program makes sure that the c++ compiler and standard library play nice together
    # with rvalue references.
    CHECK_CXX_SOURCE_COMPILES("
      #include <utility>
      #include <vector>
      #include <string>
      int main() {
        std::string str(\"txt\");
        std::vector<std::string> vec;
        vec.push_back(std::move(str));
        return 0;
      }
      " CPP11_COMPILES)
    if (CPP11_COMPILES)
      set(CPP11_SUPPORTED TRUE)
      message("Your C++ compiler appears to support C++11, setting flags to enable it.")
    else()
      set(CMAKE_CXX_FLAGS "${ORIGINAL_CXX_FLAGS}")
      set(CMAKE_REQUIRED_FLAGS "${ORIGINAL_REQUIRED_FLAGS}")
      message(WARNING "Your compiler has flags that enable C++11 support, but your C++ standard "
                      "library does not appear to support rvalue references. C++11 support will "
                      "be disabled.")
    endif()
  endif()
endif()
if(NOT CPP11_SUPPORTED)
  CHECK_CXX_COMPILER_FLAG("-std=c++0x" CXX_COMPILER_SUPPORTS_CPP0X)
  if (CXX_COMPILER_SUPPORTS_CPP0X)
    set(ORIGINAL_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    set(ORIGINAL_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}")
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS_CXX} -std=c++0x")
    # This test program makes sure that the c++ compiler and standard library play nice together
    # with rvalue references.
    CHECK_CXX_SOURCE_COMPILES("
      #include <utility>
      #include <vector>
      #include <string>
      int main() {
        std::string str(\"txt\");
        std::vector<std::string> vec;
        vec.push_back(std::move(str));
        return 0;
      }
      " CPP0X_COMPILES)
    if (CPP0X_COMPILES)
      set(CPP11_SUPPORTED TRUE)
      message(WARNING "Your C++ compiler appears to support pre-standardized C++11 (AKA C++0X). It "
                      "will be enabled, but you should consider upgrading to a more recent C++ "
                      "compiler that fully supports standard C++11.")
    else()
      set(CMAKE_CXX_FLAGS "${ORIGINAL_CXX_FLAGS}")
      set(CMAKE_REQUIRED_FLAGS "${ORIGINAL_REQUIRED_FLAGS}")
      message(WARNING "Your compiler has flags that enable pre-standardized C++11 (AKA C++0X) "
                      "support, but your C++ standard library does not appear to support rvalue "
                      "references. C++11 support will be disabled.")
    endif()
  endif()
endif()
if (NOT CPP11_SUPPORTED)
  message(WARNING "C++11 does not appear to be supported by your compiler and/or standard library. "
                  "Recent versions of GCC, Clang, ICC, and Visual Studio all support it. Consider "
                  "upgrading.")
endif()

if(COMPILER_ACTUALLY_OPEN64 OR (${CMAKE_CXX_COMPILER_ID} MATCHES "PathScale"))
  # Optimization flags for Open64 and PathScale compilers
  if (${CMAKE_CXX_COMPILER_ID} MATCHES "PathScale")
    set(COMPILER_IS_PATHSCALE TRUE)
  endif()

  # Generate all warnings for a debug build.
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -pedantic")
  # Strip binaries for release builds.
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -s")
  set(CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "${CMAKE_EXE_LINKER_FLAGS_MINSIZEREL} -s")

  # Automatically optimize for host processor.
  CHECK_CXX_COMPILER_FLAG("-march=auto" COMPILER_HAS_MARCH_AUTO)
  if (COMPILER_HAS_MARCH_AUTO)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=auto")
  endif()

  # Enable inter-procedural optimizations.
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -ipa")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ipa")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -ipa")

  set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -ipa")
  set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -ipa")
  set(CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "${CMAKE_EXE_LINKER_FLAGS_MINSIZEREL} -ipa")
elseif(CMAKE_COMPILER_IS_GNUCXX)
  # Optimization flags for GCC.

  # First, make sure we're not using an ancient GCC.
  exec_program(${CMAKE_CXX_COMPILER} ARGS --version OUTPUT_VARIABLE _gcc_version_str)
  string(REGEX MATCH "[0-9]" GCC_MAJOR_VERSION "${_gcc_version_str}")
  if (GCC_MAJOR_VERSION LESS 3)
    message(FATAL_ERROR "Your gcc/g++ version is too old to build quickstep (version 3.0 or "
                        "greater is required). If you already have a more recent gcc installed "
                        "on your system, you must specify where to find it on the command line "
                        "using \"-D CMAKE_CXX_COMPILER=/path/to/g++\" (or, alternately, set it as "
                        "the system-wide default using, for example, gcc-config on gentoo or "
                        "setgcc on haiku).")
  endif()

  # Generate all warnings for a debug build.
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -pedantic")
  # Strip binaries for release builds.
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -s")
  set(CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "${CMAKE_EXE_LINKER_FLAGS_MINSIZEREL} -s")

  # If using tcmalloc, prevent GCC from optimizing for builtin malloc and friends.
  if(USE_TCMALLOC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  endif()

  # Automatically optimize for host processor.
  CHECK_CXX_COMPILER_FLAG("-march=native" GCC_HAS_MARCH_NATIVE)
  if (GCC_HAS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
  endif()

  # Loop-transformation optimizations that aren't turned on by -O3
  CHECK_CXX_COMPILER_FLAG("-ftree-loop-linear" GCC_HAS_FTREE_LOOP_LINEAR)
  if (GCC_HAS_FTREE_LOOP_LINEAR)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -ftree-loop-linear")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ftree-loop-linear")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -ftree-loop-linear")
  endif()

  CHECK_CXX_COMPILER_FLAG("-ftree-loop-distribution" GCC_HAS_FTREE_LOOP_DISTRIBUTION)
  if (GCC_HAS_FTREE_LOOP_DISTRIBUTION)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -ftree-loop-distribution")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ftree-loop-distribution")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -ftree-loop-distribution")
  endif()

  CHECK_CXX_COMPILER_FLAG("-floop-strip-mine" GCC_HAS_FLOOP_STRIP_MINE)
  if (GCC_HAS_FLOOP_STRIP_MINE)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -floop-strip-mine")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -floop-strip-mine")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -floop-strip-mine")
  endif()

  CHECK_CXX_COMPILER_FLAG("-floop-block" GCC_HAS_FLOOP_BLOCK)
  if (GCC_HAS_FLOOP_BLOCK)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -floop-block")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -floop-block")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -floop-block")
  endif()

  # If GCC supports inter-procedural (link-time) optimizations, turn them on.
  CHECK_CXX_COMPILER_FLAG("-flto" GCC_HAS_FLTO)
  if (GCC_HAS_FLTO)
    if(CMAKE_COMPILER_IS_GNUC)
      set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -flto")
      set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -flto")
      set(CMAKE_C_FLAGS_MINSIZEREL "${CMAKE_C_FLAGS_MINSIZEREL} -flto")
    endif()

    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -flto")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -flto")

    set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -flto")
    set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -flto")
    set(CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "${CMAKE_EXE_LINKER_FLAGS_MINSIZEREL} -flto")
  endif()
elseif(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
  if(CYGWIN)
    message(WARNING "At last check, clang on cygwin doesn't play nice with exceptions, causing "
                    "quickstep builds to fail. If your build fails, try using gcc/g++ instead.")
  endif()
  # Optimization flags for Clang.

  # Generate all warnings for debug build, except for warnings about long long
  # being an extension.
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -pedantic -Wno-long-long")
  # Strip binaries for release builds.
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -s")
  set(CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "${CMAKE_EXE_LINKER_FLAGS_MINSIZEREL} -s")

  # Automatically optimize for host processor.
  CHECK_CXX_COMPILER_FLAG("-march=native" CLANG_HAS_MARCH_NATIVE)
  if(CLANG_HAS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
  endif()
elseif(${CMAKE_CXX_COMPILER_ID} MATCHES "Intel")
  # Optimization flags for the Intel C++ compiler.
  if (WIN32)
    # /fast implies /O3 /Qipo /no-prec-div
    # /QxHost automatically optimizes for host processor.
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /fast /QxHost")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} /fast /QxHost")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} /QxHost")
  else()
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -xHost")
    # Inter-procedural optimization requires using Intel's xiar tool instead of vanilla ar.
    find_program(XIAR xiar)
    if(XIAR)
      set(CMAKE_AR "${XIAR}") 

      # -xHost automatically optimizes for host processor.
      # -ipo enables inter-procedural optimization.
      # -no-prec-div enables fast floating-point division.
      # We do NOT use -fast (which implies the above 3 options), because it also forces static linking.
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -ipo -no-prec-div")
      set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ipo -no-prec-div")
    else()
      # Turn on other non-ipo optimizations.
      message(WARNING "Could not find Intel xiar tool, so turning OFF inter-procedural "
                      "optimizations for release builds.")
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -no-prec-div")
      set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -no-prec-div")
    endif()

  endif()
elseif(${CMAKE_CXX_COMPILER_ID} MATCHES "SunPro")
  # Optimization flags for SunPro (now Solaris Studio) compiler.
  set(QUICKSTEP_COMPILER_IS_SUNPRO TRUE)
  # Max out the optimization level.
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -xO5")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -xO5")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -xO5")

  # Automatically optimize for host processor.
  CHECK_CXX_COMPILER_FLAG("-xtarget=native" SUNPRO_HAS_TARGET_NATIVE)
  if (SUNPRO_HAS_TARGET_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -xtarget=native")
  endif()

  # Enable inter-procedural optimizations.
  CHECK_CXX_COMPILER_FLAG("-xipo=2" SUNPRO_HAS_IPO)
  if (SUNPRO_HAS_IPO)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -xipo=2")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -xipo=2")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -xipo=2")
  endif()

  # If possible, write-back IPO information to the library .a files (speeds up subsequent builds).
  CHECK_CXX_COMPILER_FLAG("-xipo_archive=writeback" SUNPRO_HAS_IPO_ARCHIVE_WRITEBACK)
  if (SUNPRO_HAS_IPO_ARCHIVE_WRITEBACK)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -xipo_archive=writeback")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -xipo_archive=writeback")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -xipo_archive=writeback")
  endif()

  # Enable automatic vectorization.
  CHECK_CXX_COMPILER_FLAG("-xvector=simd" SUNPRO_HAS_VECTOR_SIMD)
  if (SUNPRO_HAS_IPO_ARCHIVE_WRITEBACK)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -xvector=simd")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -xvector=simd")
    set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -xvector=simd")
  endif()
elseif(MSVC)
  # Microsoft Visual studio.
  # Work around variadic template bug.
  add_definitions( /D _VARIADIC_MAX=10 )
elseif(${CMAKE_CXX_COMPILER_ID} MATCHES "Borland")
  # Borland C++ Builder and successors (e.g. Embarcadero C++ Builder)
  set(COMPILER_IS_BORLAND TRUE)

  # Set flags to indicate we are building a console application, not a Windows GUI app.
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -tC")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -tC")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -tC")
endif()

# Set the include directories to the project root directory and the root of the build tree (where
# generated headers will go).
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Setup for third-party libs.
set(THIRD_PARTY_SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party")
include(ExternalProject)
# The third-party libs don't recognize Haiku as a variant of BeOS, so manually set the OS for them.
if(${CMAKE_SYSTEM_NAME} MATCHES "Haiku")
  set(THIRD_PARTY_CONFIG_OPTIONS "--build='i586-unknown-beos'")
endif()

# Build 64-bit libs if the quickstep build is 64-bit.
if (C_COMPILER_HAS_M64)
  set(THIRD_PARTY_CC "${CMAKE_C_COMPILER} -m64")
else()
  set(THIRD_PARTY_CC "${CMAKE_C_COMPILER}")
endif()
if (CXX_COMPILER_HAS_M64)
  set(THIRD_PARTY_CXX "${CMAKE_CXX_COMPILER} -m64")
else()
  set(THIRD_PARTY_CXX "${CMAKE_CXX_COMPILER}")
endif()

if(USE_TCMALLOC)
  if (${CMAKE_SYSTEM_NAME} MATCHES "NetBSD")
    message(WARNING "You are building with tcmalloc on NetBSD. As of NetBSD 5.1, this combination "
                    "produces an executable that always segfaults on startup. Unless you are "
                    "absolutely sure of what you are doing, you should use the system-provided "
                    "malloc instead (invoke cmake with \"-D USE_TCMALLOC=0\").")
  elseif(CYGWIN)
    message(WARNING "You are building with tcmalloc on Cygwin. This will compile fine, but it will "
                    "produce an executable which hangs on startup. Unless you are absolutely sure "
                    "of what you are doing, you should use the system-provided malloc instead "
                    "(invoke cmake with \"-D USE_TCMALLOC=0\").")
  elseif(WIN32)
    message(FATAL_ERROR "You are building with tcmalloc on Windows. Our build system can not yet "
                        "produce tcmalloc on Windows (and the tcmalloc port for Windows is "
                        "incomplete anyway). You should use the system-provided malloc instead "
                        "(invoke cmake with \"-D USE_TCMALLOC=0\").")
  elseif(${CMAKE_SYSTEM_NAME} MATCHES "Haiku")
    message(FATAL_ERROR "You are building with tcmalloc on Haiku, but tcmalloc is not compatible "
                        "with Haiku. You should use the system-provided malloc instead (invoke "
                        "cmake with \"-D USE_TCMALLOC=0\").")
  elseif(NOT UNIX)
    message(WARNING "Your system does not appear to be UNIX or Windows, but you are building with "
                    "tcmalloc. This is quite likely to fail, in which case you should use the "
                    "system-provided malloc instead (invoke cmake with \"-D USE_TCMALLOC=0\").")
  elseif(${CMAKE_CXX_COMPILER_ID} MATCHES "SunPro")
    message(WARNING "You are building tcmalloc with the sunpro compiler, which as of recent "
                    "releases fails to compile. If your build fails, you should use the "
                    "system-provided malloc instead (invoke cmake with \"-D USE_TCMALLOC=0\")")
  elseif(COMPILER_ACTUALLY_OPEN64)
    message(WARNING "You are building tcmalloc with the open64 compiler, which as of recent "
                    "releases fails to compile. If your build fails, you should use the "
                    "system-provided malloc instead (invoke cmake with \"-D USE_TCMALLOC=0\").")
  endif()

  # Build tcmalloc as a cmake ExternalProject.
  ExternalProject_Add(
    libtcmalloc_ext
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/gperftools
    SOURCE_DIR ${THIRD_PARTY_SOURCE_DIR}/gperftools
    CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --enable-minimal --disable-debugalloc ${THIRD_PARTY_CONFIG_OPTIONS} CC=${THIRD_PARTY_CC} CXX=${THIRD_PARTY_CXX}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 0
  )
  # Static libtcmalloc_minimal.a
  add_library(libtcmalloc_minimal STATIC IMPORTED)
  set_property(TARGET libtcmalloc_minimal PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/third_party/gperftools/lib/libtcmalloc_minimal.a)
  add_dependencies(libtcmalloc_minimal libtcmalloc_ext)
  set(LIBS ${LIBS} libtcmalloc_minimal)
endif()

# Add Protocol Buffers.
if(MSVC)
  if(MSVC_VERSION GREATER 1699)
    set(PROTOBUF_SOURCE_PATH "${THIRD_PARTY_SOURCE_DIR}/protobuf/vsprojects/vs2012")
  else()
    set(PROTOBUF_SOURCE_PATH "${THIRD_PARTY_SOURCE_DIR}/protobuf/vsprojects/vs2010")
  endif()
  file(TO_NATIVE_PATH "${PROTOBUF_SOURCE_PATH}" PROTOBUF_WIN_SOURCE_PATH)
  file(TO_NATIVE_PATH "${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf" PROTOBUF_WIN_INSTALL_PATH)
  ExternalProject_Add(
    protobuf_ext
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf
    SOURCE_DIR ${PROTOBUF_SOURCE_PATH}
  CONFIGURE_COMMAND echo msbuild <SOURCE_DIR>/protobuf.sln /t:protoc /t:libprotobuf-lite /p:Configuration=${CMAKE_BUILD_TYPE}
  BUILD_COMMAND msbuild <SOURCE_DIR>/protobuf.sln /t:protoc /t:libprotobuf-lite /p:Configuration=${CMAKE_BUILD_TYPE}
  INSTALL_COMMAND md ${PROTOBUF_WIN_INSTALL_PATH}\\lib
                  && md ${PROTOBUF_WIN_INSTALL_PATH}\\bin
                  && cd ${PROTOBUF_WIN_SOURCE_PATH}
                  && ${PROTOBUF_WIN_SOURCE_PATH}\\extract_includes.bat
                  && move ${PROTOBUF_WIN_SOURCE_PATH}\\include ${PROTOBUF_WIN_INSTALL_PATH}\\include
                  && copy ${PROTOBUF_WIN_SOURCE_PATH}\\${CMAKE_BUILD_TYPE}\\libprotobuf-lite.lib ${PROTOBUF_WIN_INSTALL_PATH}\\lib\\libprotobuf-lite.lib
                  && copy ${PROTOBUF_WIN_SOURCE_PATH}\\${CMAKE_BUILD_TYPE}\\protoc.exe ${PROTOBUF_WIN_INSTALL_PATH}\\bin\\protoc.exe
  BUILD_IN_SOURCE 1
  )
  add_library(libprotobuf-lite STATIC IMPORTED)
  set_property(TARGET libprotobuf-lite PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf/lib/libprotobuf-lite.lib)
  include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf/include)
  set(QS_PROTOBUF_PROTOC_EXECUTABLE ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf/bin/protoc.exe)
else()
  ExternalProject_Add(
    protobuf_ext
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf
    SOURCE_DIR ${THIRD_PARTY_SOURCE_DIR}/protobuf
    CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --disable-shared ${THIRD_PARTY_CONFIG_OPTIONS} CC=${THIRD_PARTY_CC} CXX=${THIRD_PARTY_CXX}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 0
  )
  add_library(libprotobuf-lite STATIC IMPORTED)
  set_property(TARGET libprotobuf-lite PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf/lib/libprotobuf-lite.a)
  include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf/include)
  set(QS_PROTOBUF_PROTOC_EXECUTABLE ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf/bin/protoc)
endif()
add_dependencies(libprotobuf-lite protobuf_ext)
set(LIBS ${LIBS} libprotobuf-lite)
# This function is based on a similar one from FindProtobuf.cmake
function(QS_PROTOBUF_GENERATE_CPP SRCS HDRS)
  if(NOT ARGN)
    message(SEND_ERROR "Error: QS_PROTOBUF_GENERATE_CPP() called without any proto files")
    return()
  endif()

  set(_protobuf_include_path -I${CMAKE_CURRENT_SOURCE_DIR} -I${PROJECT_SOURCE_DIR})
  set(${SRCS})
  set(${HDRS})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    list(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc")
    list(APPEND ${HDRS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h")

    add_custom_command(
      OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc"
             "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h"
      COMMAND  ${QS_PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --cpp_out  ${CMAKE_CURRENT_BINARY_DIR} ${_protobuf_include_path} ${ABS_FIL}
      DEPENDS ${ABS_FIL} protobuf_ext
      COMMENT "Running C++ protocol buffer compiler on ${FIL}"
      VERBATIM )
  endforeach()

  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()

# Add all of the module subdirectories. CMakeLists.txt in each of the subdirectories
# defines how to build that module's library.
add_subdirectory (catalog)
add_subdirectory (expressions)
add_subdirectory (storage)
add_subdirectory (threading)
add_subdirectory (types)
add_subdirectory (utility)

# Add required cmake-controlled third-party micro-libraries (cJSON).
add_subdirectory ("${THIRD_PARTY_SOURCE_DIR}/cJSON" "${CMAKE_CURRENT_BINARY_DIR}/third_party/cJSON")
set(LIBS ${LIBS} cJSON)
if (USE_INTEL_PCM)
  add_subdirectory ("${THIRD_PARTY_SOURCE_DIR}/intel-pcm" "${CMAKE_CURRENT_BINARY_DIR}/third_party/intel-pcm")
  set(LIBS ${LIBS} intel_pcm)
endif()

# Add storage explorer subdirectory.
add_subdirectory("experiments/storage_explorer")
if (POSIX_TIMERS_AVAILABLE_WITH_LRT)
  set(LIBS ${LIBS} -lrt)
endif()

# Build storage explorer binary.
add_executable(quickstep_storage_explorer experiments/storage_explorer/StorageExplorer.cpp)
target_link_libraries(quickstep_storage_explorer storageexplorer catalog expressions storage threading types ${LIBS})
