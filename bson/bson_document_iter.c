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

#include "bson_document.h"
#include "bson_document_iter.h"
#include "decoding_helpers.h"

/*
 * PyBSONDocument iterator types.
 */

/*
 * current_pos starts at 0.
 * When iteration completes, 'doc' is null.
 *
 * TODO: PyDict's iterator has a reusable PyObject *result for speed,
 *       emulate that.
 */
typedef struct {
    PyObject_HEAD
    /* Set to NULL when iterator is exhausted */
    PyBSONDocument *doc;
    /* Index into doc->keys. */
    Py_ssize_t current_pos;
    bson_iter_t bson_iter;
} bson_doc_iterobject;

static void
bson_doc_iter_dealloc(bson_doc_iterobject *iter)
{
    assert(iter);

    Py_XDECREF(iter->doc);
    PyObject_Del(iter);
}

/*
 * Get the next key from an uninflated PyBSONDocument's iterkeys(),
 * or set exception and return NULL.
 */
static PyObject *
bson_iter_nextkey_uninflated(bson_doc_iterobject *iter)
{
    PyObject *key;

    assert(iter);
    assert(iter->doc);
    assert(iter->doc->buffer);

    if (!bson_iter_next(&iter->bson_iter)) {
        /*
         * Completed.
         */
        Py_CLEAR(iter->doc);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    key = PyString_FromString(bson_iter_key(&iter->bson_iter));
    if (key) {
        ++iter->current_pos;
        return key;
    }

    return NULL;
}

/*
 * Get the next key from an inflated PyBSONDocument's iterkeys(),
 * or set exception and return NULL.
 */
static PyObject *
bson_iter_nextkey_inflated(bson_doc_iterobject *iter)
{
    PyObject *key = NULL;
    PyBSONDocument *doc;
    Py_ssize_t size;

    assert(iter);
    assert(iter->doc);
    doc = iter->doc;
    size = doc->keys ? PyList_Size(doc->keys) : 0;
    if (iter->current_pos >= size) {
        /*
         * Completed.
         */
        Py_CLEAR(iter->doc);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    key = PyList_GetItem(doc->keys, iter->current_pos);
    if (key) {
        Py_INCREF(key);
        ++iter->current_pos;
        return key;
    }

    return NULL;
}

/*
 * Get the next key from a PyBSONDocument's iterkeys().
 * iter's type is BSONDocumentIterKey_Type.
 */
static PyObject *
PyBSONDocIter_NextKey(bson_doc_iterobject *iter)
{
    PyBSONDocument *doc;

    assert(iter);

    doc = iter->doc;
    if (doc && IS_INFLATED(doc)) {
        return bson_iter_nextkey_inflated(iter);
    } else if (doc) {
        return bson_iter_nextkey_uninflated(iter);
    } else {
        /*
         * Completed: doc is null;
         */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
}

PyTypeObject PyBSONDocumentIterKey_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "PyBSONDocument key iterator",              /* tp_name */
    sizeof(bson_doc_iterobject),                /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)bson_doc_iter_dealloc,          /* tp_dealloc */
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
    (iternextfunc)PyBSONDocIter_NextKey,        /* tp_iternext */
    0,                                          /* tp_methods */
    0,                                          /* tp_members */
};

/*
 * Get the next item from an uninflated PyBSONDocument's iteritems(),
 * or set exception and return NULL.
 */
static PyObject *
bson_iter_nextitem_uninflated(bson_doc_iterobject *iter)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *result = NULL;
    PyBSONDocument *doc = iter->doc;

    assert(iter);
    assert(doc);
    assert(doc->buffer);

    if (!bson_iter_next(&iter->bson_iter)) {
        /*
         * Completed.
         */
        Py_CLEAR(iter->doc);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    key = PyString_FromString(bson_iter_key(&iter->bson_iter));
    if (!key)
        goto error;

    value = bson_iter_py_value(&iter->bson_iter, doc->buffer);
    if (!value)
        goto error;

    result = PyTuple_New(2);
    if (!result)
        goto error;

    /*
     * Steal references.
     */
    PyTuple_SET_ITEM(result, 0, key);
    PyTuple_SET_ITEM(result, 1, value);
    ++iter->current_pos;
    return result;
error:
    Py_XDECREF(key);
    Py_XDECREF(value);
    Py_XDECREF(result);
    return NULL;
}

/*
 * Get the next item from an inflated PyBSONDocument's iteritems(),
 * or set exception and return NULL.
 */
static PyObject *
bson_iter_nextitem_inflated(bson_doc_iterobject *iter)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *result = NULL;
    PyBSONDocument *doc;
    Py_ssize_t size;

    assert(iter);
    assert(iter->doc);
    doc = iter->doc;
    size = doc->keys ? PyList_Size(doc->keys) : 0;
    if (iter->current_pos >= size) {
        /*
         * Completed.
         */
        Py_CLEAR(iter->doc);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    key = PyList_GetItem(doc->keys, iter->current_pos);
    if (!key)
        goto error;

    Py_INCREF(key);
    value = PyDict_GetItem((PyObject *)doc, key);
    if (!value) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Internal error in bson_iter_nextitem_inflated.");
        goto error;
    }

    Py_INCREF(value);
    result = PyTuple_New(2);
    if (!result)
        goto error;

    /*
     * Steal references.
     */
    PyTuple_SET_ITEM(result, 0, key);
    PyTuple_SET_ITEM(result, 1, value);
    ++iter->current_pos;
    return result;
error:
    Py_XDECREF(key);
    Py_XDECREF(value);
    Py_XDECREF(result);
    return NULL;
}

/*
 * Get the next (key, value) from a PyBSONDocument's iteritems().
 * iter's type is BSONDocumentIterItem_Type.
 */
static PyObject *
PyBSONDocIter_NextItem(bson_doc_iterobject *iter)
{
    PyBSONDocument *doc;

    assert(iter);

    doc = iter->doc;
    if (doc && IS_INFLATED(doc)) {
        return bson_iter_nextitem_inflated(iter);
    } else if (doc) {
        return bson_iter_nextitem_uninflated(iter);
    } else {
        /*
         * Completed: doc is null;
         */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
}

PyTypeObject PyBSONDocumentIterItem_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "PyBSONDocument item iterator",             /* tp_name */
    sizeof(bson_doc_iterobject),                /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)bson_doc_iter_dealloc,          /* tp_dealloc */
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
    (iternextfunc)PyBSONDocIter_NextItem,       /* tp_iternext */
    0,                                          /* tp_methods */
    0,                                          /* tp_members */
};

static bson_doc_iterobject *
bson_doc_iter_new(PyBSONDocument *doc, PyTypeObject *itertype)
{
    bson_doc_iterobject *iter = PyObject_New(bson_doc_iterobject, itertype);
    if (!iter)
        goto error;

    assert(doc);

    Py_INCREF(doc);
    iter->doc = doc;
    iter->current_pos = 0;

    if (!IS_INFLATED(doc)) {
        if (!bson_iter_init(&iter->bson_iter, &doc->bson))
            goto error;
    }

    return iter;

error:
    Py_XDECREF(iter);
    return NULL;
}

PyObject *
PyBSONDocument_IterKeys(PyBSONDocument *doc)
{
    return (PyObject *)bson_doc_iter_new(doc, &PyBSONDocumentIterKey_Type);
}

PyObject *
PyBSONDocument_IterItems(PyBSONDocument *doc)
{
    return (PyObject *)bson_doc_iter_new(doc, &PyBSONDocumentIterItem_Type);
}

int
init_bson_document_iter(PyObject *module)
{
    if (PyType_Ready(&PyBSONDocumentIterKey_Type) < 0)
        return -1;

    return PyType_Ready(&PyBSONDocumentIterItem_Type);
}
