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
#include "bson.h"  // MongoDB, Inc.'s libbson project

#include "decoding_helpers.h"

PyObject *
bson_iter_py_value(bson_iter_t *iter)
{
    PyObject *ret = NULL;
    /*
     * TODO: all the BSON types.
     */
    switch (bson_iter_type(iter)) {
    case BSON_TYPE_UTF8:
        {
           bson_uint32_t utf8_len;
           const char *utf8;

           utf8 = bson_iter_utf8(iter, &utf8_len);
           if (!bson_utf8_validate(utf8, utf8_len, TRUE)) {
               /*
                * TODO: set exception.
                */
               goto error;
           }
           ret = PyString_FromString(utf8);
        }
        break;
    case BSON_TYPE_INT32:
        ret = PyLong_FromLong(bson_iter_int32(iter));
        break;
    case BSON_TYPE_INT64:
       ret = PyLong_FromLongLong(bson_iter_int64(iter));
        break;
    default:
        PyErr_SetString(PyExc_TypeError, "Unrecognized BSON type");
        goto error;
    }

    if (!ret)
        goto error;

    return ret;

error:
    Py_XDECREF(ret);
    return NULL;
}
