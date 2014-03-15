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
raise_invalid_bson_object(PyObject *value)
{
    PyObject* InvalidBSON = NULL;
    PyObject* errors = PyImport_ImportModule("bson.errors");
    if (!errors)
        goto done;
    InvalidBSON = PyObject_GetAttrString(errors, "InvalidBSON");
    if (!InvalidBSON)
        goto done;

    if (value) {
        PyErr_SetObject(InvalidBSON, value);
    } else {
        PyErr_SetString(InvalidBSON, "Buffer contained invalid BSON.");
    }

done:
    Py_XDECREF(InvalidBSON);
    Py_XDECREF(errors);
}

void
raise_invalid_bson_str(const char *msg)
{
    PyObject *value = msg ? PyString_FromString(msg) : NULL;
    raise_invalid_bson_object(value);
    Py_XDECREF(value);
}
