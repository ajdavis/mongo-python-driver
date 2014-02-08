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

#include "nodict.h"

#define INFLATED 255

/* TODO: Rename BSONDocument or something. */
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
} NoDict;

static PyObject *
NoDict_keys(PyObject *self, PyObject *args)
{
    NoDict *nodict = (NoDict *)self;
    PyObject *py_key = NULL;
    bson_t bson;
    bson_iter_t iter;
    const char *key;

    PyObject *ret = PyList_New(0);
    if (!ret) {
        goto error;
    }
    if (!bson_init_static(
            &bson,
            (bson_uint8_t *)PyByteArray_AsString(nodict->array) + nodict->offset,
            nodict->length)) {
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
 *    sets n_accesses to INFLATED
 *
 * For each PyDict_Type method, make a method that:
 *  - If this is already inflated, call dict method
 *  - If the method would modify the dict, ensure inflated and call dict method
 *  - Else increment n_accesses. If > threshhold, inflate, else use libbson
 *    method
 */
static PyMethodDef NoDict_methods[] = {
    {"keys",            (PyCFunction)NoDict_keys,         METH_NOARGS,
    "TODO"},
    {NULL,	NULL},
};

static int
NoDict_init(NoDict *self, PyObject *args, PyObject *kwds)
{
    /* TODO: shouldn't allow any args */
    if (PyDict_Type.tp_init((PyObject *)self, args, kwds) < 0) {
        return -1;
    }
    self->array = NULL;
    self->offset = 0;
    self->n_accesses = 0;
    return 0;
}

static void
NoDict_dealloc(NoDict *self)
{
    /* Free the array if this is the last NoDict using it. */
    Py_XDECREF(self->array);
    PyDict_Type.tp_free((PyObject*)self);
}

static PyTypeObject NoDict_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                       /* ob_size */
    "bson.NoDict",           /* tp_name */
    sizeof(NoDict),          /* tp_basicsize */
    0,                       /* tp_itemsize */
    (destructor)NoDict_dealloc, /* tp_dealloc */
    0,                       /* tp_print */
    0,                       /* tp_getattr */
    0,                       /* tp_setattr */
    0,                       /* tp_compare */
    0,                       /* tp_repr */
    0,                       /* tp_as_number */
    0,                       /* tp_as_sequence */
    0,                       /* tp_as_mapping */
    0,                       /* tp_hash */
    0,                       /* tp_call */
    0,                       /* tp_str */
    0,                       /* tp_getattro */
    0,                       /* tp_setattro */
    0,                       /* tp_as_buffer */
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
_cbson_load_from_bytearray(PyObject *self, PyObject *args) {
    PyObject *ret = NULL;
    PyObject *array = NULL;
    PyObject *init_args = NULL;
    NoDict *nodict = NULL;
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

    while (1) {
        Py_XDECREF(nodict);
        /* PyType_GenericNew seems unable to create a dict or a subclass of it. */
        nodict = (NoDict*)PyObject_Call((PyObject *)&NoDict_Type, init_args, NULL);
        if (!nodict) {
            bson_reader_destroy(reader);
            goto error;
        }

        nodict->array = array;
        Py_INCREF(array);
        nodict->offset = offset;
        b = bson_reader_read(reader, &eof);
        if (!b) {
            /* Finished. */
            nodict->length = buffer_size - nodict->offset;
            break;
        }

        /* Continuing. */
        offset = bson_reader_tell(reader);
        nodict->length = offset - nodict->offset;
        if (PyList_Append(ret, (PyObject*)nodict) < 0) {
            bson_reader_destroy(reader);
            goto error;
        }
    }

    Py_CLEAR(nodict);
    bson_reader_destroy(reader);

    if (!eof) {
       PyErr_SetString(PyExc_ValueError, "Buffer contained invalid BSON.");
       goto error;
    }

    return ret;

error:
    Py_XDECREF(ret);
    Py_XDECREF(array);
    Py_XDECREF(nodict);
    Py_XDECREF(init_args);
    return NULL;
}

int
init_nodict(PyObject* module)
{
    NoDict_Type.tp_base = &PyDict_Type;
    if (PyType_Ready(&NoDict_Type) < 0) {
        return -1;
    }

    Py_INCREF(&NoDict_Type);
    if (PyModule_AddObject(module, "NoDict", (PyObject *) &NoDict_Type) < 0) {
        return -1;
    }
    return 0;
}
