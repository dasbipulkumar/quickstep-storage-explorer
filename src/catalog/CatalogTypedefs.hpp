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

#ifndef QUICKSTEP_CATALOG_CATALOG_TYPEDEFS_HPP_
#define QUICKSTEP_CATALOG_CATALOG_TYPEDEFS_HPP_

#include <climits>

// In all cases, a negative value indicates an invalid/unset id

namespace quickstep {

/** \addtogroup Catalog
 *  @{
 */

typedef int database_id;
typedef int relation_id;
typedef int attribute_id;

// This depends on all the above id types being typedefed to int.
const int kCatalogMaxID = INT_MAX;

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_CATALOG_CATALOG_TYPEDEFS_HPP_
