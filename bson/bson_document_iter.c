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

#include "bson_document.h"
#include "bson_document_iter.h"
#include "decoding_helpers.h"

/*
 * BSONDocument iterator types.
 */

/*
 * TODO: PyDict's iterator has a reusable PyObject *result for speed,
 *       emulate that.
 */
typedef struct {
    PyObject_HEAD
    BSONDocument *doc; /* Set to NULL when iterator is exhausted */
    bson_t bson;
    bson_iter_t bson_iter;
} bson_doc_iterobject;

static void
BSONDocIter_Dealloc(bson_doc_iterobject *iter)
{
    Py_XDECREF(iter->doc);
    PyObject_Del(iter);
}

/*
 * Get the next (key, value) from a BSONDocument's iteritems().
 * iter's type is BSONDocumentIterItem_Type.
 */
static PyObject *BSONDocIter_NextItem(bson_doc_iterobject *iter)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *result = NULL;
    bson_iter_t *bson_iter;
    BSONDocument *doc = iter->doc;
    bson_iter = &iter->bson_iter;

    /*
     * TODO: check if doc was modified and raise.
     * TODO: somehow continue if doc was inflated?
     */

    if (doc && bson_iter_next(&iter->bson_iter)) {
        key = PyString_FromString(bson_iter_key(bson_iter));
        if (!key)
            goto error;

        value = bson_iter_py_value(bson_iter, doc->buffer);
        if (!value)
            goto error;

        result = PyTuple_New(2);
        if (!result)
            goto error;

        PyTuple_SET_ITEM(result, 0, key);
        PyTuple_SET_ITEM(result, 1, value);
        return result;
    } else {
        Py_CLEAR(iter->doc);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

error:
    Py_XDECREF(key);
    Py_XDECREF(value);
    Py_XDECREF(result);
    return NULL;
}

PyTypeObject BSONDocumentIterItem_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "BSONDocument iterator",                    /* tp_name */
    sizeof(bson_doc_iterobject),                /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)BSONDocIter_Dealloc,            /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                         /* tp_flags */
    0,                                          /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    PyObject_SelfIter,                          /* tp_iter */
    (iternextfunc)BSONDocIter_NextItem,         /* tp_iternext */
    0,                                          /* tp_methods */
    0,                                          /* tp_members */
};

static bson_doc_iterobject *
bson_doc_iter_new(BSONDocument *doc, PyTypeObject *itertype)
{
    bson_uint8_t *buffer_ptr;
    bson_doc_iterobject *iter = PyObject_New(bson_doc_iterobject, itertype);
    if (!iter)
        goto error;

    /*
     * We only need to refer to 'doc' so it isn't dealloc'ed before us.
     */
    Py_INCREF(doc);
    iter->doc = doc;

    /*
     * TODO: refactor. And what if doc is inflated?
     */
    buffer_ptr = (bson_uint8_t *)PyByteArray_AsString(doc->buffer->array)
                 + doc->offset;

    if (!bson_init_static(
            &iter->bson,
            buffer_ptr,
            doc->length)) {
        goto error;
    }

    if (!bson_iter_init(&iter->bson_iter, &iter->bson))
        goto error;

    return iter;

error:
    Py_XDECREF(iter);
    return NULL;
}

/*
 * TODO: iterkeys and itervalues.
 */

PyObject *
BSONDoc_IterItems(BSONDocument *doc)
{
    return (PyObject *)bson_doc_iter_new(doc, &BSONDocumentIterItem_Type);
}

int
init_bson_document_iter(PyObject *module)
{
    return PyType_Ready(&BSONDocumentIterItem_Type);
}
