/*
 * Copyright 2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Python.h"

#include "invalid_bson.h"

void
raise_invalid_bson(const char *msg)
{
    PyObject* InvalidBSON;
    PyObject* errors = PyImport_ImportModule("bson.errors");
    if (!errors)
        return;
    InvalidBSON = PyObject_GetAttrString(errors, "InvalidBSON");
    Py_DECREF(errors);
    if (!InvalidBSON)
        return;

    PyErr_SetString(
            InvalidBSON,
            msg ? msg : "Buffer contained invalid BSON.");

    Py_DECREF(InvalidBSON);
}
