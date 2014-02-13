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
 * Before iterating, current_key is NULL and current_pos is -1. While iterating
 * an uninflated BSONDocument, current_key is the last returned key. If doc
 * inflates, its list of keys is filled out. On our next iteration, current_key
 * is cleared and current_pos is set.
 *
 * When iteration completes, 'doc' is null.
 *
 * TODO: PyDict's iterator has a reusable PyObject *result for speed,
 *       emulate that.
 */
typedef struct {
    PyObject_HEAD
    /* Set to NULL when iterator is exhausted */
    BSONDocument *doc;
    /* Last seen key. */
    PyObject *current_key;
    /* Index into doc->keys. */
    Py_ssize_t current_pos;
    bson_and_iter_t bson_and_iter;
} bson_doc_iterobject;

static void
BSONDocIter_Dealloc(bson_doc_iterobject *iter)
{
    Py_XDECREF(iter->doc);
    PyObject_Del(iter);
}

/*
 * Get the next item from an uninflated BSONDocument's iteritems(),
 * or set exception and return NULL.
 */
static PyObject *
bson_iter_nextitem_uninflated(bson_doc_iterobject *iter)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *result = NULL;
    bson_and_iter_t *bson_and_iter;
    BSONDocument *doc = iter->doc;
    bson_and_iter = &iter->bson_and_iter;

    if (!bson_iter_next(&bson_and_iter->iter)) {
        /*
         * Completed.
         */
        Py_CLEAR(iter->doc);
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    key = PyString_FromString(bson_iter_key(&bson_and_iter->iter));
    if (!key)
        goto error;

    value = bson_iter_py_value(&bson_and_iter->iter, doc->buffer);
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

    /*
     * Remember our current key, in case doc inflates before next iteration.
     */
    Py_XDECREF(iter->current_key);
    Py_INCREF(key);
    iter->current_key = key;
    return result;
error:
    Py_XDECREF(key);
    Py_XDECREF(value);
    Py_XDECREF(result);
    return NULL;
}

/*
 * Get the next item from an inflated BSONDocument's iteritems(),
 * or set exception and return NULL.
 */
static PyObject *
bson_iter_nextitem_inflated(bson_doc_iterobject *iter)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *result = NULL;
    BSONDocument *doc = iter->doc;
    Py_ssize_t size = PyList_Size(doc->keys);

    ++iter->current_pos;
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

    value = PyDict_GetItem((PyObject *)doc, key);
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
    return result;
error:
    Py_XDECREF(key);
    Py_XDECREF(value);
    Py_XDECREF(result);
    return NULL;
}

/*
 * Return index or -1.
 */
static Py_ssize_t
list_find(PyObject *list, PyObject *item)
{
    Py_ssize_t size = PyList_Size(list);
    Py_ssize_t i;
    for (i = 0; i < size; ++i) {
        PyObject *current = PyList_GetItem(list, i);
        if (!current)
            /* Huh? */
            return -1;

        /*
         * TODO: Is this the best way to compare? Matches dict semantics?
         */
        if (PyObject_RichCompareBool(item, current, Py_EQ))
            return i;
    }

    return -1;
}

/*
 * Convert from an iterator over an uninflated doc to an inflated one,
 * preserving position, or set exception and return FALSE;
 */
static int
bson_iter_inflate(bson_doc_iterobject *iter)
{
    /*
     * TODO: Assert doc->keys.
     */
    Py_ssize_t pos = list_find(iter->doc->keys, iter->current_key);
    if (pos == -1) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Internal error in bson_iter_inflate");
        goto error;
    }

    iter->current_pos = pos;
    Py_CLEAR(iter->current_key);

    return TRUE;

error:
    return FALSE;
}

/*
 * Get the next (key, value) from a BSONDocument's iteritems().
 * iter's type is BSONDocumentIterItem_Type.
 */
static PyObject *
BSONDocIter_NextItem(bson_doc_iterobject *iter)
{
    BSONDocument *doc = iter->doc;
    if (doc && IS_INFLATED(doc)) {
        if (iter->current_key)
            /*
             * First iteration since the doc was inflated.
             */
            bson_iter_inflate(iter);

        /*
         * TODO: Check if doc was modified and raise.
         */
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
    bson_doc_iterobject *iter = PyObject_New(bson_doc_iterobject, itertype);
    if (!iter)
        goto error;

    Py_INCREF(doc);
    iter->doc = doc;
    iter->current_key = NULL;
    iter->current_pos = -1;

    if (!IS_INFLATED(doc)) {
        if (!bson_doc_iter_init(doc, &iter->bson_and_iter))
            goto error;
    }

    return iter;

error:
    Py_XDECREF(iter);
    return NULL;
}

/*
 * TODO: iterkeys and itervalues.
 */

PyObject *
BSONDocument_IterItems(BSONDocument *doc)
{
    return (PyObject *)bson_doc_iter_new(doc, &BSONDocumentIterItem_Type);
}

int
init_bson_document_iter(PyObject *module)
{
    return PyType_Ready(&BSONDocumentIterItem_Type);
}
