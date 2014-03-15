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
#include "bson.h"

#include "decoding_helpers.h"
#include "bson_document.h"
#include "invalid_bson.h"

#define JAVA_LEGACY   5
#define CSHARP_LEGACY 6

/*
 * Initialize a bson_t and bson_iter_t, or set exception and return 0.
 */
int
bson_doc_iter_init(PyBSONDocument *doc, bson_and_iter_t *bson_and_iter)
{
    if (!doc->buffer)
        goto error;

    uint8_t *buffer_ptr =
            (uint8_t *)PyByteArray_AsString(doc->buffer->array)
            + doc->offset;

    if (!bson_init_static(
            &bson_and_iter->bson,
            buffer_ptr,
            doc->length))
        goto error;

    if (!bson_iter_init(&bson_and_iter->iter, &bson_and_iter->bson))
        goto error;

    return 1;

error:
    PyErr_SetString(PyExc_RuntimeError,
                    "Internal error in bson_doc_iter_init.");
    return 0;
}

/*
 * Return bytes (Python 3) or str (Python 2), or set exception and return NULL.
 */
static PyObject *
binary_data_to_py_bytes(
        const uint8_t *binary_data,
        uint32_t binary_len)
{
#if PY_MAJOR_VERSION >= 3
    return PyBytes_FromStringAndSize((const char *)binary_data, binary_len);
#else
    return PyString_FromStringAndSize((const char *)binary_data, binary_len);
#endif
}

static void
_fix_java(const uint8_t *in, uint8_t *out) {
    int i, j;
    for (i = 0, j = 7; i < j; i++, j--) {
        out[i] = in[j];
        out[j] = in[i];
    }
    for (i = 8, j = 15; i < j; i++, j--) {
        out[i] = in[j];
        out[j] = in[i];
    }
}

/*
 * Return a UUID object or set exception and return NULL.
 */
static PyObject *
bson_data_to_uuid(const uint8_t *binary_data, int uuid_subtype)
{
    PyObject *data = binary_data_to_py_bytes(binary_data, 16);
    PyObject *kwargs = PyDict_New();
    PyObject *args = PyTuple_New(0);
    PyObject *uuid_module = NULL;
    PyObject *uuid_class = NULL;
    PyObject *ret = NULL;

    if (!data || !args || !kwargs)
        goto done;

    if (uuid_subtype == CSHARP_LEGACY) {
        /* Legacy C# byte order */
        if ((PyDict_SetItemString(kwargs, "bytes_le", data)) < 0)
            goto done;
    }
    else {
        if (uuid_subtype == JAVA_LEGACY) {
            /* Convert from legacy java byte order */
            uint8_t big_endian[16];
            _fix_java(binary_data, big_endian);
            /* Free the previously created PyString object */
            Py_DECREF(data);
            data = binary_data_to_py_bytes(big_endian, 16);
            if (!data)
                goto done;
        }
        if ((PyDict_SetItemString(kwargs, "bytes", data)) < 0)
            goto done;
    }

    uuid_module = PyImport_ImportModule("uuid");
    if (!uuid_module)
        goto done;

    uuid_class = PyObject_GetAttrString(uuid_module, "UUID");
    if (!uuid_class)
        goto done;

    ret = PyObject_Call(uuid_class, args, kwargs);

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
        const uint8_t *binary_data,
        uint32_t binary_len,
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
bson_iter_to_binary(bson_iter_t *iter, int uuid_subtype)
{
    bson_subtype_t binary_subtype;
    uint32_t binary_len;
    const uint8_t *binary_data;
    PyObject *ret = NULL;

    bson_iter_binary(
        iter,
        &binary_subtype,
        &binary_len,
        &binary_data);

    if (!binary_data) {
        raise_invalid_bson_str("Invalid BSON binary object");
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

        ret = bson_data_to_uuid(binary_data, uuid_subtype);
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
 * Decode an ObjectId, or return NULL and set exception.
 */
static PyObject *
bson_iter_to_objectid(bson_iter_t *iter)
{
    PyObject *ret = NULL;
    PyObject *objectid_module = NULL;
    PyObject *objectid_class = NULL;
    PyObject *args = NULL;
    const bson_oid_t *oid;
    PyObject *oid_bytes = NULL;


    /*
     * TODO: use libbson's ObjectId instead of ours.
     */
    objectid_module = PyImport_ImportModule("bson.objectid");
    if (!objectid_module)
        goto done;

    objectid_class = PyObject_GetAttrString(objectid_module, "ObjectId");
    if (!objectid_class)
        goto done;

    oid = bson_iter_oid(iter);
    if (!oid)
        goto done;


#if PY_MAJOR_VERSION >= 3
    oid_bytes = PyBytes_FromStringAndSize((const char *)oid->bytes, 12);
#else
    oid_bytes = PyString_FromStringAndSize((const char *)oid->bytes, 12);
#endif

    args = PyTuple_New(1);
    if (!args)
        goto done;

    PyTuple_SET_ITEM(args, 0, oid_bytes);
    /* args has stolen reference to oid_bytes. */
    oid_bytes = NULL;

    ret = PyObject_Call(objectid_class, args, NULL);

done:
    Py_XDECREF(objectid_module);
    Py_XDECREF(objectid_class);
    Py_XDECREF(oid_bytes);
    Py_XDECREF(args);

    return ret;
}

/*
 * Decode the value at the current position, or set exception and return NULL.
 */
PyObject *
bson_iter_py_value(bson_iter_t *iter, PyBSONBuffer *buffer)
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
    case BSON_TYPE_UTF8:
        {
           uint32_t utf8_len;
           const char *utf8;

           utf8 = bson_iter_utf8(iter, &utf8_len);
           if (!bson_utf8_validate(utf8, utf8_len, 1)) {
               raise_invalid_bson_str("Invalid UTF8 string");
               goto done;
           }
           ret = PyString_FromString(utf8);
        }
        break;
    case BSON_TYPE_DOCUMENT:
        {
            PyBSONDocument *doc;
            off_t start;
            uint8_t *buffer_ptr =
                    (uint8_t *)PyByteArray_AsString(buffer->array);

            bson_iter_t child_iter;
            if (!bson_iter_recurse(iter, &child_iter)) {
                raise_invalid_bson_str("Invalid subdocument");
                goto done;
            }

            start = (off_t)(child_iter.raw - buffer_ptr);
            doc = PyBSONDocument_New(
                    buffer,
                    start,
                    (off_t)(start + child_iter.len));

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
                raise_invalid_bson_str("Invalid array");
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
        ret = bson_iter_to_binary(iter, buffer->uuid_subtype);
        break;
    case BSON_TYPE_OID:
        ret = bson_iter_to_objectid(iter);
        break;
    case BSON_TYPE_INT32:
        ret = PyInt_FromLong(bson_iter_int32(iter));
        break;
    case BSON_TYPE_INT64:
        ret = PyLong_FromLongLong(bson_iter_int64(iter));
        break;
    default:
        {
            PyObject *msg = PyString_FromFormat("Unrecognized BSON type: %d",
                                                bson_iter_type(iter));
            raise_invalid_bson_object(msg);
            Py_XDECREF(msg);
        }
        break;
    }

done:
    return ret;
}
