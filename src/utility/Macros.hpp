/*
   This file copyright (c) 2011-2013, the Quickstep authors.
   See file CREDITS.txt for details.
  
   This file is part of Quickstep.

   Quickstep is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
  
   Quickstep is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Quickstep.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef QUICKSTEP_UTILITY_MACROS_HPP_
#define QUICKSTEP_UTILITY_MACROS_HPP_

#ifdef QUICKSTEP_DEBUG
#include <cassert>
#endif
#include <cstdlib>
#include <iostream>

/**
 * This macro prevents instances of a class from being copied or assigned
 * by automatically-generated (dumb, shallow) copy constructors and assignment
 * operators. It should be placed in a class' private section, and given the
 * name of the class as its argument.
 **/
#define DISALLOW_COPY_AND_ASSIGN(classname) \
  classname(const classname &orig);\
  classname & operator=(const classname &rhs)

/**
 * This macro calls assert() with the given statement when QUICKSTEP_DEBUG is
 * defined (i.e. this is a debug build), otherwise it is a no-op. Remember: for
 * release builds, the provided statement is NOT called, so don't use this for
 * statements that have any side effects.
 **/
#ifdef QUICKSTEP_DEBUG
#define DEBUG_ASSERT(statement) \
  assert(statement)
#else
#define DEBUG_ASSERT(statement)
#endif

/**
 * This macro always calls the provided statement. When QUICKSTEP_DEBUG is
 * defined (i.e. this is a debug build), an assertion checks that the provided
 * statement returns 0.
 **/
#ifdef QUICKSTEP_DEBUG
#define DO_AND_DEBUG_ASSERT_ZERO(statement) \
  assert(statement == 0)
#else
#define DO_AND_DEBUG_ASSERT_ZERO(statement) \
  statement
#endif

/**
 * This macro logs the provided error message to STDERR and causes the program
 * to exit. We use a macro instead of a function so that compilers will realize
 * that calling FATAL_ERROR terminates the program, and it is OK to not return
 * from the calling function.
 **/
#define FATAL_ERROR(message) \
  std::cerr << "FATAL ERROR: " << message << "\n";\
  std::exit(1)

/**
 * This macro logs a warning message to STDERR. Note that race conditions are
 * possible when multiple threads call this, potentially garbling output.
 **/
#define LOG_WARNING(message) \
  std::cerr << "WARNING: " << message << "\n";

#endif  // QUICKSTEP_UTILITY_MACROS_HPP_
