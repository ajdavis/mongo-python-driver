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
#include "utlist.h"
#include "invalid_bson.h"

#define SHOULD_INFLATE(doc) (doc->n_accesses >= 10)

/*
 * Detach doc from buffer.
 */
void
bson_doc_detach_buffer(BSONDocument *doc)
{
    if (doc->buffer) {
        DL_DELETE(doc->buffer->dependents, doc);

        /* Not reference-counted. */
        doc->buffer = NULL;
    }
}

/*
 * Replace linear access with a hash table, or set exception and return FALSE.
 * Does not release the buffer.
 */
int
bson_doc_inflate(BSONDocument *doc)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    bson_and_iter_t bson_and_iter;
    if (IS_INFLATED(doc))
        return TRUE;

    assert(!doc->keys);
    doc->keys = PyList_New(0);
    if (!doc->keys)
        goto error;

    if (!bson_doc_iter_init(doc, &bson_and_iter))
        goto error;

    while (bson_iter_next(&bson_and_iter.iter)) {
        const char *ckey = bson_iter_key(&bson_and_iter.iter);
        if (!ckey) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Internal error in bson_doc_inflate.");
            goto error;
        }
        key = PyString_FromString(ckey);
        if (!key) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Internal error in bson_doc_inflate.");
            goto error;
        }

        value = bson_iter_py_value(&bson_and_iter.iter, doc->buffer);
        if (!value)
            /* Exception is already set. */
            goto error;

        if (PyDict_SetItem((PyObject *)doc, key, value) < 0)
            goto error;

        /* Remember key order. */
        if (PyList_Append(doc->keys, key) < 0)
            goto error;

        Py_CLEAR(key);
        Py_CLEAR(value);
    }

    bson_doc_detach_buffer(doc);
    return TRUE;

error:
    PyDict_Clear((PyObject *)doc);
    Py_CLEAR(doc->keys);
    Py_XDECREF(key);
    Py_XDECREF(value);
    return FALSE;
}

/*
 * Call inflate() and release the buffer, or set exception and return FALSE.
 */
int
bson_doc_detach(BSONDocument *doc)
{
    if (!bson_doc_inflate(doc))
        return FALSE;

    /* Not reference-counted. */
    doc->buffer = NULL;
    return TRUE;
}

static Py_ssize_t
BSONDocument_Size(BSONDocument *doc)
{
    Py_ssize_t ret = 0;
    bson_and_iter_t bson_and_iter;
    if (!IS_INFLATED(doc)) {
        ++doc->n_accesses;
        if (SHOULD_INFLATE(doc)) {
            if (!bson_doc_detach(doc))
                goto error;
        }
    }

    if (IS_INFLATED(doc))
        return PyDict_Size((PyObject *)doc);

    if (!bson_doc_iter_init(doc, &bson_and_iter))
        goto error;

    while (bson_iter_next(&bson_and_iter.iter)) ++ret;
    return ret;

error:
    return -1;
}

static PyObject *
BSONDocument_Subscript(PyObject *self, PyObject *key)
{
    BSONDocument *doc = (BSONDocument *)self;
    PyObject *ret = NULL;
    bson_and_iter_t bson_and_iter;
    const char *cstring_key = NULL;

    if (!IS_INFLATED(doc)) {
        ++doc->n_accesses;
        if (SHOULD_INFLATE(doc)) {
            if (!bson_doc_detach(doc))
                goto error;
        }
    }

    if (IS_INFLATED(doc)) {
        ret = PyDict_GetItem((PyObject *)doc, key);
        if (!ret) {
            /* PyDict_GetItem doesn't set exception. */
            PyErr_SetObject(PyExc_KeyError, key);
            goto error;
        }

        /*
         * PyDict_GetItem returns borrowed reference.
         */
        Py_INCREF(ret);
        return ret;
    }

    cstring_key = PyString_AsString(key);
    if (!cstring_key) {
        goto error;
    }
    if (!bson_doc_iter_init(doc, &bson_and_iter)) {
        /*
         * TODO: Raise InvalidBSON. Refactor error-raising code from
         * _cbsonmodule.c and bson_buffer.c first.
         */
        goto error;
    }
    /*
     * TODO: we could use bson_iter_find_with_len if it were public.
     */
    if (!bson_iter_find(&bson_and_iter.iter, cstring_key)) {
        PyErr_SetObject(PyExc_KeyError, key);
        goto error;
    }

    ret = bson_iter_py_value(&bson_and_iter.iter, doc->buffer);
    if (!ret)
        goto error;

    return ret;

error:
    Py_XDECREF(ret);
    return NULL;
}

static int
BSONDocument_AssignSubscript(BSONDocument *doc, PyObject *v, PyObject *w)
{
    if (!bson_doc_detach(doc))
        return -1;

    /*
     * Put this key last.
     */
    if (PyList_Append(doc->keys, v) < 0)
        return -1;

    return PyDict_SetItem((PyObject *)doc, v, w);
}

static PyObject *
BSONDocument_Keys(PyObject *self, PyObject *args)
{
    BSONDocument *doc = (BSONDocument *)self;
    PyObject *py_key = NULL;
    bson_and_iter_t bson_and_iter;
    const char *key;
    PyObject *ret = NULL;

    if (!IS_INFLATED(doc)) {
        ++doc->n_accesses;
        if (SHOULD_INFLATE(doc)) {
            if (!bson_doc_detach(doc))
                goto error;
        }
    }

    if (IS_INFLATED(doc))
        return PyDict_Keys((PyObject *)doc);

    ret = PyList_New(0);
    if (!ret)
        goto error;

    if (!bson_doc_iter_init(doc, &bson_and_iter))
        goto error;

    while (bson_iter_next(&bson_and_iter.iter)) {
        key = bson_iter_key(&bson_and_iter.iter);
        if (!key) {
            raise_invalid_bson("Invalid key.");
            goto error;
        }
        py_key = PyString_FromString(key);
        if (!py_key)
            goto error;

        if (PyList_Append(ret, py_key) < 0)
            goto error;
    }

    return ret;

error:
    Py_XDECREF(ret);
    Py_XDECREF(py_key);
    return NULL;
}

/*
 * TODO: For each PyDict_Type method, make a method that:
 *  - If this is already inflated, call dict method
 *  - If the method would modify, ensure inflated and call dict method
 *  - Else increment n_accesses and call libbson method
 *
 * BSONDocument must also be iterable, with the same shortcut methods as
 * BSONBuffer.
 */

static PyObject *
BSONDocument_Inflate(BSONDocument *doc)
{
    if (!bson_doc_inflate(doc))
        return NULL;

    Py_RETURN_NONE;
}

static PyObject *
BSONDocument_Inflated(BSONDocument *doc)
{
    if (IS_INFLATED(doc)) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

PyDoc_STRVAR(getitem__doc__,
             "x.__getitem__(y) <==> x[y]");
PyDoc_STRVAR(keys__doc__,
             "D.keys() -> list of D's keys");
PyDoc_STRVAR(iteritems__doc__,
             "D.iteritems() -> an iterator over the (key, value) items of D");
PyDoc_STRVAR(inflate__doc__,
             "Turn into a hashtable and release buffer");
PyDoc_STRVAR(inflated__doc__,
             "True once this document is inflated into a hashtable");

static PyMethodDef BSONDocument_methods[] = {
        /*
         * PyDict-like methods
         */
//    {"__contains__",    (PyCFunction)bson_doc_contains,     METH_O | METH_COEXIST,
//     contains__doc__},
    {"__getitem__",     (PyCFunction)BSONDocument_Subscript, METH_O | METH_COEXIST,
     getitem__doc__},
//    {"__sizeof__",      (PyCFunction)bson_doc_sizeof,       METH_NOARGS,
//     sizeof__doc__},
//    {"has_key",         (PyCFunction)bson_doc_has_key,      METH_O,
//     has_key__doc__},
//    {"get",             (PyCFunction)bson_doc_get,          METH_VARARGS,
//     get__doc__},
//    {"setdefault",      (PyCFunction)bson_doc_setdefault,   METH_VARARGS,
//     setdefault_doc__},
//    {"pop",             (PyCFunction)bson_doc_pop,          METH_VARARGS,
//     pop__doc__},
//    {"popitem",         (PyCFunction)bson_doc_popitem,      METH_NOARGS,
//     popitem__doc__},
    {"keys",            (PyCFunction)BSONDocument_Keys, METH_NOARGS,
     keys__doc__},
//    {"items",           (PyCFunction)bson_doc_items,        METH_NOARGS,
//     items__doc__},
//    {"values",          (PyCFunction)bson_doc_values,       METH_NOARGS,
//     values__doc__},
//    {"viewkeys",        (PyCFunction)dictkeys_new,      METH_NOARGS,
//     viewkeys__doc__},
//    {"viewitems",       (PyCFunction)dictitems_new,     METH_NOARGS,
//     viewitems__doc__},
//    {"viewvalues",      (PyCFunction)dictvalues_new,    METH_NOARGS,
//     viewvalues__doc__},
//    {"update",          (PyCFunction)bson_doc_update,       METH_VARARGS | METH_KEYWORDS,
//     update__doc__},
//    {"fromkeys",        (PyCFunction)bson_doc_fromkeys,     METH_VARARGS | METH_CLASS,
//     fromkeys__doc__},
//    {"clear",           (PyCFunction)bson_doc_clear,        METH_NOARGS,
//     clear__doc__},
//    {"copy",            (PyCFunction)bson_doc_copy,         METH_NOARGS,
//     copy__doc__},
//    {"iterkeys",        (PyCFunction)bson_doc_iterkeys,     METH_NOARGS,
//     iterkeys__doc__},
//    {"itervalues",      (PyCFunction)bson_doc_itervalues,   METH_NOARGS,
//     itervalues__doc__},
    {"iteritems",       (PyCFunction)BSONDocument_IterItems, METH_NOARGS,
     iteritems__doc__},
     /*
      * BSONDocument-specific methods
      */
    {"inflate",         (PyCFunction)BSONDocument_Inflate, METH_NOARGS,
     inflate__doc__},
    {"inflated",        (PyCFunction)BSONDocument_Inflated, METH_NOARGS,
     inflated__doc__},
    {NULL,	NULL},
};

static PyMappingMethods bson_doc_as_mapping = {
    (lenfunc)BSONDocument_Size,          /* mp_length */
    (binaryfunc)BSONDocument_Subscript,  /* mp_subscript */
    (objobjargproc)BSONDocument_AssignSubscript, /* mp_ass_subscript */
};

static void
BSONDocument_Dealloc(BSONDocument* doc)
{
    bson_doc_detach_buffer(doc);
    PyDict_Type.tp_dealloc((PyObject *)doc);
}

static int
BSONDocument_init(BSONDocument *doc, PyObject *args, PyObject *kwds)
{
	/*
	 * TODO: accept a buffer, offset, and length.
	 */
    if ((args && PyObject_Length(args) > 0)
    		|| (kwds && PyObject_Length(kwds) > 0)) {
    	PyErr_SetString(PyExc_TypeError, "BSONDocument takes no arguments");
    	return -1;
    }
    if (PyDict_Type.tp_init((PyObject *)doc, args, kwds) < 0) {
        return -1;
    }
    doc->buffer = NULL;
    doc->offset = 0;
    doc->length = 0;
    doc->n_accesses = 0;
    return 0;
}

static PyTypeObject BSONDocument_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                       /* ob_size */
    "bson.BSONDocument",     /* tp_name */
    sizeof(BSONDocument),    /* tp_basicsize */
    0,                       /* tp_itemsize */
    (destructor)BSONDocument_Dealloc, /* tp_dealloc */
    0,                       /* tp_print */
    0,                       /* tp_getattr */
    0,                       /* tp_setattr */
    0,                       /* tp_compare */
    0,                       /* tp_repr */
    0,                       /* tp_as_number */
    0,                       /* tp_as_sequence */
    &bson_doc_as_mapping,    /* tp_as_mapping */
    0,                       /* tp_hash */
    0,                       /* tp_call */
    0,                       /* tp_str */
    0,                       /* tp_getattro */
    0,                       /* tp_setattro */
    0,                       /* tp_as_buffer */
    /* TODO: Py_TPFLAGS_DICT_SUBCLASS ? */
    Py_TPFLAGS_DEFAULT |
      Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "TODO",                  /* tp_doc */
    0,                       /* tp_traverse */
    0,                       /* tp_clear */
    0,                       /* tp_richcompare */
    0,                       /* tp_weaklistoffset */
    0,                       /* tp_iter */
    0,                       /* tp_iternext */
    BSONDocument_methods,    /* tp_methods */
    0,                       /* tp_members */
    0,                       /* tp_getset */
    0,                       /* tp_base */
    0,                       /* tp_dict */
    0,                       /* tp_descr_get */
    0,                       /* tp_descr_set */
    0,                       /* tp_dictoffset */
    (initproc)BSONDocument_init, /* tp_init */
    0,                       /* tp_alloc */
    0,                       /* tp_new */
};

/*
 * TODO: start and length might be cleaner than start and end.
 */
BSONDocument *
BSONDocument_New(BSONBuffer *buffer, bson_off_t start, bson_off_t end)
{
    BSONDocument *doc;
    PyObject *init_args = NULL;

    /*
     * TODO: cache.
     */
    init_args = PyTuple_New(0);
    if (!init_args)
        return NULL;

    /*
     * PyType_GenericNew seems unable to create a dict or a subclass of it.
     * TODO: Why? I'm confused about the relationship of this and
     *       BSONDocument_init.
     */
    doc = (BSONDocument*)PyObject_Call(
        (PyObject *)&BSONDocument_Type, init_args, NULL);

    Py_DECREF(init_args);

    if (!doc)
        return NULL;

    /* Not reference-counted. */
    doc->buffer = buffer;
    doc->offset = start;
    doc->length = end - start;
    doc->keys = NULL;
    return doc;
}

int
init_bson_document(PyObject* module)
{
    BSONDocument_Type.tp_base = &PyDict_Type;
    if (PyType_Ready(&BSONDocument_Type) < 0)
        return -1;

    return 0;
}
