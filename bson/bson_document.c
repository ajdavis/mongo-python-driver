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
#include "bson.h"  /* MongoDB, Inc.'s libbson project. */

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
bson_doc_detach_buffer(PyBSONDocument *doc)
{
    if (doc->buffer) {
        DL_DELETE(doc->buffer->dependents, doc);

        /* Not reference-counted. */
        doc->buffer = NULL;
    }
}

/*
 * Replace linear access with a hash table, or set exception and return 0.
 * Does not release the buffer.
 */
int
bson_doc_inflate(PyBSONDocument *doc)
{
    PyObject *key = NULL;
    PyObject *value = NULL;
    bson_and_iter_t bson_and_iter;
    if (IS_INFLATED(doc))
        return 1;

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
    return 1;

error:
    PyDict_Clear((PyObject *)doc);
    Py_CLEAR(doc->keys);
    Py_XDECREF(key);
    Py_XDECREF(value);
    return 0;
}

/*
 * Call inflate() and release the buffer, or set exception and return 0.
 */
int
bson_doc_detach(PyBSONDocument *doc)
{
    if (!bson_doc_inflate(doc))
        return 0;

    /* Not reference-counted. */
    doc->buffer = NULL;
    return 1;
}

static Py_ssize_t
PyBSONDocument_Size(PyBSONDocument *doc)
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
PyBSONDocument_Subscript(PyObject *self, PyObject *key)
{
    PyBSONDocument *doc = (PyBSONDocument *)self;
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

/*
 * Return index or -1.
 */
static Py_ssize_t
list_find(PyObject *list, PyObject *item)
{
    Py_ssize_t size = PyList_Size(list);
    Py_ssize_t i;

    assert(list);
    assert(item);

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

static int
PyBSONDocument_AssignSubscript(PyBSONDocument *doc, PyObject *v, PyObject *w)
{
    if (!bson_doc_detach(doc))
        return -1;

    if (w) {
        /*
         * Put this key last.
         */
        if (PyList_Append(doc->keys, v) < 0)
            return -1;

        return PyDict_SetItem((PyObject *)doc, v, w);
    } else {
        /*
         * w is NULL: del document['field'].
         */
        Py_ssize_t index = list_find(doc->keys, v);
        if (index < 0) {
            PyErr_SetObject(PyExc_KeyError, v);
            return -1;
        }

        if (PyDict_DelItem((PyObject *)doc, v) < 0)
            return -1;

        /*
         * Remove from list of keys. Failure is unexpected and very bad, since
         * we've already modified the dict.
         */
        assert(index < PyList_Size(doc->keys));
        return PyList_SetSlice(doc->keys, index, index + 1, NULL);
    }
}

static PyObject *
PyBSONDocument_Keys(PyObject *self, PyObject *args)
{
    PyBSONDocument *doc = (PyBSONDocument *)self;
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
            raise_invalid_bson_str("Invalid key.");
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
 * Adapted from dict_repr().
 */
static PyObject *
bson_doc_repr(PyBSONDocument *doc)
{
    Py_ssize_t i;
    PyObject *s = NULL;
    PyObject *iter = NULL;
    PyObject *temp = NULL;
    PyObject *colon = NULL;
    PyObject *pieces = NULL;
    PyObject *result = NULL;
    PyObject *pair = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;

    /* Avoid recursion. */
    i = Py_ReprEnter((PyObject *)doc);
    if (i) {
        return i > 0 ? PyString_FromString("{...}") : NULL;
    }

    pieces = PyList_New(0);
    if (!pieces)
        goto done;

    colon = PyString_FromString(": ");
    if (!colon)
        goto done;

    iter = PyBSONDocument_IterItems(doc);
    if (!iter)
        goto done;

    while ((pair = PyIter_Next(iter))) {
        int status;

        /* Borrowed references. */
        key = PyTuple_GetItem(pair, 0);
        if (!key)
            goto done;

        value = PyTuple_GetItem(pair, 1);
        if (!value)
            goto done;

        s = PyObject_Repr(key);
        PyString_Concat(&s, colon);
        PyString_ConcatAndDel(&s, PyObject_Repr(value));
        if (!s)
            goto done;

        status = PyList_Append(pieces, s);
        Py_CLEAR(s);
        if (status < 0)
            goto done;
    }

    /*
     * PyIter_Next returns NULL on normal termination and on error.
     */
    if (PyErr_Occurred())
        goto done;

    if (PyList_GET_SIZE(pieces) == 0) {
        result = PyString_FromString("{}");
        goto done;
    }

    s = PyString_FromString("{");
    if (!s)
        goto done;

    temp = PyList_GET_ITEM(pieces, 0);
    PyString_ConcatAndDel(&s, temp);
    PyList_SET_ITEM(pieces, 0, s);
    if (!s)
        goto done;

    s = PyString_FromString("}");
    if (!s)
        goto done;

    temp = PyList_GET_ITEM(pieces, PyList_GET_SIZE(pieces) - 1);
    PyString_ConcatAndDel(&temp, s);
    PyList_SET_ITEM(pieces, PyList_GET_SIZE(pieces) - 1, temp);
    if (!temp)
        goto done;

    temp = NULL;

    /* Paste them all together with ", " between. */
    s = PyString_FromString(", ");
    if (!s)
        goto done;

    result = _PyString_Join(s, pieces);

done:
    Py_XDECREF(s);
    Py_XDECREF(iter);
    Py_XDECREF(temp);
    Py_XDECREF(colon);
    Py_XDECREF(pieces);
    Py_XDECREF(pair);
    Py_ReprLeave((PyObject *)doc);
    return result;
}

/*
 * TODO: For each PyDict_Type method, make a method that:
 *  - If this is already inflated, call dict method
 *  - If the method would modify, ensure inflated and call dict method
 *  - Else increment n_accesses and call libbson method
 *
 * PyBSONDocument must also be iterable, with the same shortcut methods as
 * BSONBuffer.
 */

static PyObject *
PyBSONDocument_Inflate(PyBSONDocument *doc)
{
    if (!bson_doc_inflate(doc))
        return NULL;

    Py_RETURN_NONE;
}

static PyObject *
PyBSONDocument_Inflated(PyBSONDocument *doc)
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
    {"__getitem__",     (PyCFunction)PyBSONDocument_Subscript, METH_O | METH_COEXIST,
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
    {"keys",            (PyCFunction)PyBSONDocument_Keys, METH_NOARGS,
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
    {"iterkeys",        (PyCFunction)PyBSONDocument_IterKeys, METH_NOARGS },
//    {"itervalues",      (PyCFunction)bson_doc_itervalues,   METH_NOARGS,
//     itervalues__doc__},
    {"iteritems",       (PyCFunction)PyBSONDocument_IterItems, METH_NOARGS,
     iteritems__doc__},
     /*
      * PyBSONDocument-specific methods
      */
    {"inflate",         (PyCFunction)PyBSONDocument_Inflate, METH_NOARGS,
     inflate__doc__},
    {"inflated",        (PyCFunction)PyBSONDocument_Inflated, METH_NOARGS,
     inflated__doc__},
    {NULL,	NULL},
};

static PyMappingMethods bson_doc_as_mapping = {
    (lenfunc)PyBSONDocument_Size,          /* mp_length */
    (binaryfunc)PyBSONDocument_Subscript,  /* mp_subscript */
    (objobjargproc)PyBSONDocument_AssignSubscript, /* mp_ass_subscript */
};

/*
 * Return 1 if key is in doc, 0 if not, or set exception and return -1.
 */
static int
PyBSONDocument_Contains(PyBSONDocument *doc, PyObject *key)
{
    /* Assume error. */
    int ret = -1;

    /* TODO: refactor */
    if (!IS_INFLATED(doc)) {
        ++doc->n_accesses;
        if (SHOULD_INFLATE(doc)) {
            if (!bson_doc_detach(doc))
                goto done;
        }
    }

    if (IS_INFLATED(doc)) {
        ret = PyDict_Contains((PyObject *)doc, key);
        goto done;
    } else {
        bson_and_iter_t bson_and_iter;
        const char *cstring_key = PyString_AsString(key);
        if (!cstring_key)
            goto done;

        if (!bson_doc_iter_init(doc, &bson_and_iter)) {
            raise_invalid_bson_str(NULL);
            goto done;
        }

        /*
         * TODO: we could use bson_iter_find_with_len if it were public.
         */
        ret = bson_iter_find(&bson_and_iter.iter, cstring_key);
        goto done;
    }

done:
    return ret;
}

/*
 * Hack to implement "key in document".
 * Copied from dictobject.c.
 */
static PySequenceMethods document_as_sequence = {
    0,                          /* sq_length */
    0,                          /* sq_concat */
    0,                          /* sq_repeat */
    0,                          /* sq_item */
    0,                          /* sq_slice */
    0,                          /* sq_ass_item */
    0,                          /* sq_ass_slice */
    (objobjproc)PyBSONDocument_Contains, /* sq_contains */
    0,                          /* sq_inplace_concat */
    0,                          /* sq_inplace_repeat */
};

static void
bson_doc_dealloc(PyBSONDocument* doc)
{
    bson_doc_detach_buffer(doc);
    PyDict_Type.tp_dealloc((PyObject *)doc);
}

static int
bson_doc_init(PyBSONDocument *doc, PyObject *args, PyObject *kwds)
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

static PyTypeObject PyBSONDocument_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                       /* ob_size */
    "bson.BSONDocument",     /* tp_name */
    sizeof(PyBSONDocument),    /* tp_basicsize */
    0,                       /* tp_itemsize */
    (destructor)bson_doc_dealloc, /* tp_dealloc */
    0,                       /* tp_print */
    0,                       /* tp_getattr */
    0,                       /* tp_setattr */
    0,                       /* tp_compare */
    (reprfunc)bson_doc_repr, /* tp_repr */
    0,                       /* tp_as_number */
    &document_as_sequence,   /* tp_as_sequence */
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
    (getiterfunc)PyBSONDocument_IterKeys, /* tp_iter */
    0,                       /* tp_iternext */
    BSONDocument_methods,    /* tp_methods */
    0,                       /* tp_members */
    0,                       /* tp_getset */
    0,                       /* tp_base */
    0,                       /* tp_dict */
    0,                       /* tp_descr_get */
    0,                       /* tp_descr_set */
    0,                       /* tp_dictoffset */
    (initproc)bson_doc_init, /* tp_init */
    0,                       /* tp_alloc */
    0,                       /* tp_new */
};

/*
 * TODO: start and length might be cleaner than start and end.
 */
PyBSONDocument *
PyBSONDocument_New(PyBSONBuffer *buffer, off_t start, off_t end)
{
    PyBSONDocument *doc;
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
     *       PyBSONDocument_init.
     */
    doc = (PyBSONDocument*)PyObject_Call(
        (PyObject *)&PyBSONDocument_Type, init_args, NULL);

    Py_DECREF(init_args);

    if (!doc)
        return NULL;

    /* Buffer is not reference-counted. */
    doc->buffer = buffer;
    doc->keys = NULL;
    doc->offset = start;
    /*
     * TODO: check for overflow.
     */
    doc->length = end - start;
    return doc;
}

int
init_bson_document(PyObject* module)
{
    PyBSONDocument_Type.tp_base = &PyDict_Type;
    if (PyType_Ready(&PyBSONDocument_Type) < 0)
        return -1;

    Py_INCREF(&PyBSONDocument_Type);
    if (PyModule_AddObject(module,
                           "BSONDocument",
                           (PyObject *)&PyBSONDocument_Type) < 0)
        return -1;

    return 0;
}
