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
#include "bson_document.h"

/*
 * Initialize a bson_t and bson_iter_t, or set exception and return FALSE.
 */
int
bson_doc_iter_init(BSONDocument *doc, bson_and_iter_t *bson_and_iter)
{
    if (!doc->buffer)
        goto error;

    bson_uint8_t *buffer_ptr =
            (bson_uint8_t *)PyByteArray_AsString(doc->buffer->array)
            + doc->offset;

    if (!bson_init_static(
            &bson_and_iter->bson,
            buffer_ptr,
            doc->length))
        goto error;

    if (!bson_iter_init(&bson_and_iter->iter, &bson_and_iter->bson))
        goto error;

    return TRUE;

error:
    PyErr_SetString(PyExc_RuntimeError,
                    "Internal error in bson_doc_iter_init.");
    return FALSE;
}

/*
 * Decode the value at the current position, or set exception and return NULL.
 */
PyObject *
bson_iter_py_value(bson_iter_t *iter, BSONBuffer *buffer)
{
    PyObject *ret = NULL;
    /*
     * TODO: all the BSON types.
     */
    switch (bson_iter_type(iter)) {
    case BSON_TYPE_DOUBLE:
        ret = PyFloat_FromDouble(bson_iter_double(iter));
        break;
    case BSON_TYPE_DOCUMENT:
        {
            BSONDocument *doc;
            bson_off_t start;
            bson_uint8_t *buffer_ptr =
                    (bson_uint8_t *)PyByteArray_AsString(buffer->array);

            bson_iter_t child_iter;
            if (!bson_iter_recurse(iter, &child_iter)) {
                PyErr_SetString(PyExc_ValueError, "invalid subdocument");
                goto error;
            }

            start = (bson_off_t)(child_iter.raw - buffer_ptr);
            doc = BSONDocument_New(
                    buffer,
                    start,
                    (bson_off_t)(start + child_iter.len));

            if (doc)
                bson_buffer_attach_doc(buffer, doc);

            ret = (PyObject *)doc;
        }
        break;
    case BSON_TYPE_ARRAY:
        {
            /*
             * TODO: could make a new lazy type here.
             */
            bson_iter_t child_iter;
            if (!bson_iter_recurse(iter, &child_iter)) {
                /*
                 * TODO: everything should be InvalidBSON.
                 */
                PyErr_SetString(PyExc_ValueError, "invalid array");
                goto error;
            }
            ret = PyList_New(0);
            if (!ret)
                goto error;

            while (bson_iter_next(&child_iter)) {
                PyObject *element = bson_iter_py_value(&child_iter, buffer);
                if (!element)
                    goto error;

                if (PyList_Append(ret, element) < 0) {
                    Py_DECREF(element);
                    goto error;
                }
            }
        }
        break;
    case BSON_TYPE_UTF8:
        {
           bson_uint32_t utf8_len;
           const char *utf8;

           utf8 = bson_iter_utf8(iter, &utf8_len);
           if (!bson_utf8_validate(utf8, utf8_len, TRUE)) {
               PyErr_SetString(PyExc_ValueError, "invalid utf8 string");
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
        PyErr_SetString(PyExc_ValueError, "Unrecognized BSON type");
        goto error;
    }

    if (!ret)
        /* The exception has already been set. */
        goto error;

    return ret;

error:
    Py_XDECREF(ret);
    return NULL;
}
