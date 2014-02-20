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
#include "invalid_bson.h"

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
 * Return bytes (Python 3) or str (Python 2), or set exception and return NULL.
 */
static PyObject *
binary_data_to_py_bytes(
        const bson_uint8_t *binary_data,
        bson_uint32_t binary_len)
{
#if PY_MAJOR_VERSION >= 3
    return PyBytes_FromStringAndSize((const char *)binary_data, binary_len);
#else
    return PyString_FromStringAndSize((const char *)binary_data, binary_len);
#endif
}

/*
 * Return a UUID object or set exception and return NULL.
 */
static PyObject *
bson_data_to_uuid(const bson_uint8_t *binary_data)
{
    PyObject *data = binary_data_to_py_bytes(binary_data, 16);
    PyObject *kwargs = PyDict_New();
    PyObject *args = PyTuple_New(0);
    PyObject *uuid_module = NULL;
    PyObject *uuid_class = NULL;
    PyObject *ret = NULL;

    if (!data || !args || !kwargs)
        goto done;

    /*
     * TODO: JAVA_LEGACY and CSHARP_LEGACY.
     */
    if (PyDict_SetItemString(kwargs, "bytes", data) < 0)
        goto done;

    uuid_module = PyImport_ImportModule("uuid");
    if (!uuid_module)
        goto done;

    uuid_class = PyObject_GetAttrString(uuid_module, "UUID");
    if (!uuid_class)
        goto done;

    ret = PyObject_Call(uuid_class, args, kwargs);
    if (!ret)
        goto done;

done:
    Py_XDECREF(data);
    Py_XDECREF(args);
    Py_XDECREF(kwargs);
    Py_XDECREF(uuid_module);
    Py_XDECREF(uuid_class);

    return ret;
}

/*
 * Return a Python Binary object, or set exception and return NULL.
 */
static PyObject *
bson_data_to_binary(
        const bson_uint8_t *binary_data,
        bson_uint32_t binary_len,
        bson_subtype_t binary_subtype)
{
    PyObject *binary_module = NULL;
    PyObject *binary_class = NULL;
    PyObject *subtype_obj = NULL;
    PyObject *data = NULL;
    PyObject *ret = NULL;

    binary_module  = PyImport_ImportModule("bson.binary");
    if (!binary_module)
       goto done;

    binary_class = PyObject_GetAttrString(binary_module, "Binary");
    if (!binary_class)
       goto done;

#if PY_MAJOR_VERSION >= 3
    subtype_obj = PyLong_FromLong(binary_subtype);
#else
    subtype_obj = PyInt_FromLong(binary_subtype);
#endif
    if (!subtype_obj)
        goto done;

    data = binary_data_to_py_bytes(binary_data, binary_len);
    if (!data)
        goto done;

    ret = PyObject_CallFunctionObjArgs(binary_class, data, subtype_obj, NULL);

done:
    Py_XDECREF(subtype_obj);
    Py_XDECREF(data);
    Py_XDECREF(binary_module);
    Py_XDECREF(binary_class);
    return ret;
}
/*
 * Decode a BSON binary, or return NULL and set exception.
 */
static PyObject *
bson_iter_to_binary(bson_iter_t *iter) {
    bson_subtype_t binary_subtype;
    bson_uint32_t binary_len;
    const bson_uint8_t *binary_data;
    PyObject *ret = NULL;

    bson_iter_binary(
        iter,
        &binary_subtype,
        &binary_len,
        &binary_data);

    if (!binary_data) {
        raise_invalid_bson("Invalid BSON binary object");
        goto done;
    }

    /*
     * Encode as UUID, not Binary.
     */
    if (binary_subtype == BSON_SUBTYPE_UUID
            || binary_subtype == BSON_SUBTYPE_UUID_DEPRECATED) {
        /* UUID should always be 16 bytes. */
        if (binary_len != 16)
            goto done;

        ret = bson_data_to_uuid(binary_data);
        if (!ret)
            goto done;
    }

#if PY_MAJOR_VERSION >= 3
    /*
     * Python3 special case. Decode BSON binary subtype 0 to bytes.
     * */
    if (binary_subtype == BSON_SUBTYPE_BINARY
            || binary_subtype == BSON_SUBTYPE_BINARY_DEPRECATED) {
        ret = binary_data_to_py_bytes(binary_data, binary_len);
        if (!ret)
            goto done;
    }
#endif

    if (!ret) {
        /*
         * We haven't thrown an error or made a UUID or bytes.
         * Make a standard Binary.
         */
        ret = bson_data_to_binary(binary_data, binary_len, binary_subtype);
    }

done:
    return ret;
}

/*
 * Decode the value at the current position, or set exception and return NULL.
 */
PyObject *
bson_iter_py_value(bson_iter_t *iter, BSONBuffer *buffer)
{
    PyObject *ret = NULL;
    /*
     * TODO: All the BSON types.
     * TODO: Use types from libbson's Python bindings instead of PyMongo's
     *       Python types.
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
                raise_invalid_bson("Invalid subdocument");
                goto done;
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
                raise_invalid_bson("Invalid array");
                goto done;
            }
            ret = PyList_New(0);
            if (!ret)
                goto done;

            while (bson_iter_next(&child_iter)) {
                PyObject *element = bson_iter_py_value(&child_iter, buffer);
                if (!element)
                    goto done;

                if (PyList_Append(ret, element) < 0) {
                    Py_DECREF(element);
                    goto done;
                }
            }
        }
        break;
    case BSON_TYPE_BINARY:
        ret = bson_iter_to_binary(iter);
        break;
    case BSON_TYPE_UTF8:
        {
           bson_uint32_t utf8_len;
           const char *utf8;

           utf8 = bson_iter_utf8(iter, &utf8_len);
           if (!bson_utf8_validate(utf8, utf8_len, TRUE)) {
               raise_invalid_bson("Invalid UTF8 string");
               goto done;
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
        raise_invalid_bson("Unrecognized BSON type");
        goto done;
    }

done:
    return ret;
}
