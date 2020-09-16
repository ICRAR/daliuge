//
// Data structures and functions definitions for DALiuGE dynamic libraries
// implementing a DropApp
//
// ICRAR - International Centre for Radio Astronomy Research
// (c) UWA - The University of Western Australia, 2020
// Copyright by UWA (in the framework of the ICRAR)
// All rights reserved
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston,
// MA 02111-1307  USA
//

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#include "dlg_app.h"

/**
 * Initializes a new application. Expects a dictionary containing all the parameters.
 *
 * @param app The new application instance
 * @param params A PyObject pointer to a a PyObject. it is up to the imp
 * @return A python object which could contain an Python Exception, or a success (0)
 *         or fail (any other value)
 */
PyObject* init2(dlg_app_info *app, PyObject* params);

/**
 * Provides a simple entry point to execute batch-oriented applications.
 *
 * @param app The application instance
 * @return A python object which could contain an Python Exception, or a success (0)
 *         or fail (any other value)
 */
PyObject* run2(dlg_app_info *app);


#ifdef __cplusplus
}
#endif
