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

#include "bson_document.h"

#define INFLATED 255

static Py_ssize_t
bson_doc_length(BSONDocument *doc)
{
    Py_ssize_t ret = 0;
    bson_t bson;
    bson_iter_t iter;
    bson_uint8_t *buffer_ptr;
    buffer_ptr = (bson_uint8_t *)PyByteArray_AsString(doc->array)
                 + doc->offset;

    if (!bson_init_static(
            &bson,
            buffer_ptr,
            doc->length)) {
        goto error;
    }
    if (!bson_iter_init(&iter, &bson)) {
        bson_destroy(&bson);
        goto error;
    }

    while (bson_iter_next(&iter)) ++ret;
    bson_destroy(&bson);
    return ret;

error:
    return -1;
}

static PyObject *
bson_doc_subscript(PyObject *self, PyObject *key)
{
    BSONDocument *doc = (BSONDocument *)self;
    PyObject *ret = NULL;
    const char *cstring_key;
    unsigned char bson_initialized = FALSE;
    bson_t bson;
    bson_iter_t iter;
    bson_uint8_t *buffer_ptr;

    cstring_key = PyString_AsString(key);
    if (!cstring_key) {
        goto error;
    }
    buffer_ptr = (bson_uint8_t *)PyByteArray_AsString(doc->array)
                 + doc->offset;

    if (!bson_init_static(
            &bson,
            buffer_ptr,
            doc->length)) {
        goto error;
    }
    bson_initialized = TRUE;
    if (!bson_iter_init(&iter, &bson)) {
        /*
         * TODO: Raise InvalidBSON. Refactor error-raising code from
         * _cbsonmodule.c and bson_document_iterator.c first.
         */
        goto error;
    }
    /*
     * TODO: we could use bson_iter_find_with_len if it were public.
     */
    if (!bson_iter_find(&iter, cstring_key)) {
        PyErr_SetObject(PyExc_KeyError, key);
        goto error;
    }

    /*
     * TODO: all the BSON types.
     */
    switch (bson_iter_type(&iter)) {
    case BSON_TYPE_UTF8:
		{
		   bson_uint32_t utf8_len;
		   const char *utf8;

		   utf8 = bson_iter_utf8(&iter, &utf8_len);
		   if (!bson_utf8_validate(utf8, utf8_len, TRUE)) {
		       /*
		        * TODO: set exception.
		        */
		       goto error;
		   }
		   ret = PyString_FromString(utf8);
		}
		break;
    default:
        PyErr_SetString(PyExc_TypeError, "Unrecognized BSON type");
        goto error;
    }

	if (bson_initialized) {
        bson_destroy(&bson);
	}
    return ret;

error:
    if (bson_initialized) {
        bson_destroy(&bson);
    }
    Py_XDECREF(ret);
    return NULL;

}

static int
bson_doc_ass_sub(PyDictObject *mp, PyObject *v, PyObject *w)
{
    /*
     * TODO
     */
    return -1;
}

static PyObject *
bson_doc_keys(PyObject *self, PyObject *args)
{
    BSONDocument *doc = (BSONDocument *)self;
    PyObject *py_key = NULL;
    bson_t bson;
    bson_iter_t iter;
    const char *key;
    bson_uint8_t *buffer_ptr;

    PyObject *ret = PyList_New(0);
    if (!ret) {
        goto error;
    }
    buffer_ptr = (bson_uint8_t *)PyByteArray_AsString(doc->array)
                 + doc->offset;

    if (!bson_init_static(
            &bson,
            buffer_ptr,
            doc->length)) {
        goto error;
    }

    if (!bson_iter_init(&iter, &bson)) {
        bson_destroy(&bson);
        goto error;
    }

    while (bson_iter_next(&iter)) {
        key = bson_iter_key(&iter);
        if (!key) {
            bson_destroy(&bson);
            goto error;
        }
        py_key = PyString_FromString(key);
        if (!py_key) {
            bson_destroy(&bson);
            goto error;
        }

        if (PyList_Append(ret, py_key) < 0) {
            bson_destroy(&bson);
            goto error;
        }
    }

    bson_destroy(&bson);
    return ret;

error:
    Py_XDECREF(ret);
    Py_XDECREF(py_key);
    return NULL;
}

/*
 * TODO:
 *  - Write an 'inflate' method that creates the dict and releases the buffer,
 *    sets n_accesses to INFLATED.
 *  - Or perhaps it's not n_accesses that matters but the number of keys, or
 *    size of the document.
 *
 * For each PyDict_Type method, make a method that:
 *  - If this is already inflated, call dict method
 *  - If the method would modify the dict, ensure inflated and call dict method
 *  - Else increment n_accesses. If > threshhold, inflate, else use libbson
 *    method
 */

PyDoc_STRVAR(getitem__doc__, "x.__getitem__(y) <==> x[y]");
PyDoc_STRVAR(keys__doc__, "D.keys() -> list of D's keys");

static PyMethodDef BSONDocument_methods[] = {
//    {"__contains__",    (PyCFunction)bson_doc_contains,     METH_O | METH_COEXIST,
//     contains__doc__},
    {"__getitem__",     (PyCFunction)bson_doc_subscript,    METH_O | METH_COEXIST,
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
    {"keys",            (PyCFunction)bson_doc_keys,         METH_NOARGS,
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
//    {"iteritems",       (PyCFunction)bson_doc_iteritems,    METH_NOARGS,
//     iteritems__doc__},
    {NULL,	NULL},
};

static PyMappingMethods bson_doc_as_mapping = {
    (lenfunc)bson_doc_length, /* mp_length */
    (binaryfunc)bson_doc_subscript, /* mp_subscript */
    (objobjargproc)bson_doc_ass_sub, /* mp_ass_subscript */
};

static int
BSONDocument_init(BSONDocument *self, PyObject *args, PyObject *kwds)
{
	/*
	 * TODO: accept an array, offset, and length.
	 */
    if ((args && PyObject_Length(args) > 0)
    		|| (kwds && PyObject_Length(kwds) > 0)) {
    	PyErr_SetString(PyExc_TypeError, "BSONDocument takes no arguments");
    	return -1;
    }
    if (PyDict_Type.tp_init((PyObject *)self, args, kwds) < 0) {
        return -1;
    }
    self->array = NULL;
    self->offset = 0;
    self->length = 0;
    self->n_accesses = 0;
    return 0;
}

static void
BSONDocument_dealloc(BSONDocument *self)
{
    /* Free the array if this is the last BSONDocument using it. */
    Py_XDECREF(self->array);
    PyDict_Type.tp_free((PyObject*)self);
}

static PyTypeObject BSONDocument_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                       /* ob_size */
    "bson.BSONDocument",           /* tp_name */
    sizeof(BSONDocument),          /* tp_basicsize */
    0,                       /* tp_itemsize */
    (destructor)BSONDocument_dealloc, /* tp_dealloc */
    0,                       /* tp_print */
    0,                       /* tp_getattr */
    0,                       /* tp_setattr */
    0,                       /* tp_compare */
    0,                       /* tp_repr */
    0,                       /* tp_as_number */
    0,                       /* tp_as_sequence */
    &bson_doc_as_mapping,      /* tp_as_mapping */
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
    BSONDocument_methods,          /* tp_methods */
    0,                       /* tp_members */
    0,                       /* tp_getset */
    0,                       /* tp_base */
    0,                       /* tp_dict */
    0,                       /* tp_descr_get */
    0,                       /* tp_descr_set */
    0,                       /* tp_dictoffset */
    (initproc)BSONDocument_init,   /* tp_init */
    0,                       /* tp_alloc */
    0,                       /* tp_new */
};

BSONDocument *
bson_doc_new(PyObject *array, bson_off_t start, bson_off_t end)
{
    BSONDocument *doc;
    PyObject *init_args = NULL;
    if (!PyByteArray_Check(array)) {
        PyErr_SetString(PyExc_TypeError, "BSONDocument requires a bytearray");
        return NULL;
    }

    /*
     * TODO: cache.
     */
    init_args = PyTuple_New(0);
    if (!init_args) {
        return NULL;
    }

    /*
     * PyType_GenericNew seems unable to create a dict or a subclass of it.
     * TODO: why?
     */
    doc = (BSONDocument*)PyObject_Call(
        (PyObject *)&BSONDocument_Type, init_args, NULL);

    Py_DECREF(init_args);

    if (!doc) {
        return NULL;
    }

    Py_INCREF(array);
    doc->array = array;
    doc->offset = start;
    doc->length = end - start;
    return doc;
}

int
init_bson_document(PyObject* module)
{
    BSONDocument_Type.tp_base = &PyDict_Type;
    if (PyType_Ready(&BSONDocument_Type) < 0) {
        return -1;
    }

    Py_INCREF(&BSONDocument_Type);
    if (PyModule_AddObject(module, "BSONDocument",
                           (PyObject *)&BSONDocument_Type) < 0) {
        return -1;
    }
    return 0;
}
