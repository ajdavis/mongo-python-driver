/*
 * Copyright 2009-2014 MongoDB, Inc.
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
#include <bson.h>  // MongoDB, Inc.'s libbson project

#include "bson_document.h"

#define INFLATED 255

typedef struct {
    /* Superclass. */
    PyDictObject dict;
    /* bytearray from which we're reading */
    PyObject *array;
    /* This document's offset into array */
    bson_off_t offset;
    /* This document's length */
    bson_size_t length;
    /* How many times have we been accessed? */
    unsigned char n_accesses;
} BSONDocument;

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
    /*
     * TODO: right?
     */
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
        goto error;
    }
    if (!bson_iter_find(&iter, cstring_key)) {
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

static PyMethodDef NoDict_methods[] = {
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
NoDict_init(BSONDocument *self, PyObject *args, PyObject *kwds)
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
NoDict_dealloc(BSONDocument *self)
{
    /* Free the array if this is the last BSONDocument using it. */
    Py_XDECREF(self->array);
    PyDict_Type.tp_free((PyObject*)self);
}

static PyTypeObject NoDict_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                       /* ob_size */
    "bson.BSONDocument",           /* tp_name */
    sizeof(BSONDocument),          /* tp_basicsize */
    0,                       /* tp_itemsize */
    (destructor)NoDict_dealloc, /* tp_dealloc */
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
    NoDict_methods,          /* tp_methods */
    0,                       /* tp_members */
    0,                       /* tp_getset */
    0,                       /* tp_base */
    0,                       /* tp_dict */
    0,                       /* tp_descr_get */
    0,                       /* tp_descr_set */
    0,                       /* tp_dictoffset */
    (initproc)NoDict_init,   /* tp_init */
    0,                       /* tp_alloc */
    0,                       /* tp_new */
};

PyObject *
load_from_bytearray(PyObject *self, PyObject *args) {
    PyObject *ret = NULL;
    PyObject *array = NULL;
    PyObject *init_args = NULL;
    BSONDocument *doc = NULL;
    /* TODO: must this be on the heap? */
    bson_reader_t *reader;
    const bson_t *b;
    bson_bool_t eof = FALSE;
    bson_off_t offset = 0;
    bson_size_t buffer_size;

    if (!PyArg_ParseTuple(args, "O", &array)) {
        return NULL;
    }
    if (!PyByteArray_Check(array)) {
        PyErr_SetString(PyExc_TypeError, "array must be a bytearray.");
        goto error;
    }
    ret = PyList_New(0);
    if (!ret) {
        goto error;
    }
    init_args = PyTuple_New(0); /* TODO: cache this? */
    if (!init_args) {
        goto error;
    }

    buffer_size = PyByteArray_Size(array);
    reader = bson_reader_new_from_data(
            (bson_uint8_t *)PyByteArray_AsString(array),
            buffer_size);

    if (!reader) {
        goto error;
    }

    while (TRUE) {
        Py_XDECREF(doc);
        /*
         * PyType_GenericNew seems unable to create a dict or a subclass of it.
         * TODO: why?
         */
        doc = (BSONDocument*)PyObject_Call(
            (PyObject *)&NoDict_Type, init_args, NULL);
        if (!doc) {
            bson_reader_destroy(reader);
            goto error;
        }

        doc->array = array;
        Py_INCREF(array);
        doc->offset = offset;
        b = bson_reader_read(reader, &eof);
        if (!b) {
            /* Finished. */
            doc->length = buffer_size - doc->offset;
            break;
        }

        /* Continuing. */
        offset = bson_reader_tell(reader);
        doc->length = offset - doc->offset;
        if (PyList_Append(ret, (PyObject*)doc) < 0) {
            bson_reader_destroy(reader);
            goto error;
        }
    }

    Py_CLEAR(doc);
    bson_reader_destroy(reader);

    if (!eof) {
       PyErr_SetString(PyExc_ValueError, "Buffer contained invalid BSON.");
       goto error;
    }

    return ret;

error:
    Py_XDECREF(ret);
    Py_XDECREF(array);
    Py_XDECREF(doc);
    Py_XDECREF(init_args);
    return NULL;
}

int
init_bson_document(PyObject* module)
{
    NoDict_Type.tp_base = &PyDict_Type;
    if (PyType_Ready(&NoDict_Type) < 0) {
        return -1;
    }

    Py_INCREF(&NoDict_Type);
    if (PyModule_AddObject(
            module, "BSONDocument", (PyObject *) &NoDict_Type) < 0) {
        return -1;
    }
    return 0;
}
