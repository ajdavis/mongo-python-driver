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

#include <Python.h>

#include <bson.h>  // MongoDB, Inc.'s libbson project

#include "bson_document.h"
#include "bson_document_iterator.h"

typedef struct {
    PyObject_HEAD
    bson_reader_t *reader;
} BSONDocumentIterator;

PyObject *
bson_doc_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}

PyObject *
bson_doc_iternext(PyObject *self) {
    BSONDocumentIterator *iter = (BSONDocumentIterator *)self;
    if (FALSE) {
        /*
         * TODO
         */
    } else {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
}

static PyTypeObject BSONDocumentIterator_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "bson.BSONDocumentIterator", /*tp_name*/
    sizeof(BSONDocumentIterator), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    0,                         /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    /* Py_TPFLAGS_HAVE_ITER tells Python to
       use tp_iter and tp_iternext fields. */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
    "BSONDocument lazy-loading iterator.", /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    bson_doc_iter,                 /* tp_iter: __iter__() method */
    bson_doc_iternext              /* tp_iternext: next() method */
};

static PyObject *
bson_doc_iter_init(PyObject *self, PyObject *args)
{
    BSONDocumentIterator *ret = NULL;
    if (args && PyObject_Size(args) > 0) {
        PyErr_SetString(PyExc_TypeError,
                        "BSONDocumentIterator takes no arguments");
        return NULL;
    }

    /* I don't need Python callable __init__() method for this iterator,
     so I'll simply allocate it as PyObject and initialize it by hand. */
    ret = PyObject_New(BSONDocumentIterator, &BSONDocumentIterator_Type);
    if (!ret) {
        return NULL;
    }

    if (!PyObject_Init((PyObject *)ret, &BSONDocumentIterator_Type)) {
        Py_DECREF(ret);
        return NULL;
    }

    /*
     * TODO: finish initialization.
     */
    return (PyObject *)ret;
}

PyObject *
load_from_bytearray(PyObject *self, PyObject *args) {
    PyObject *array = NULL;
    PyObject *ret = NULL;
    BSONDocument *doc = NULL;
    /* TODO: must this be on the heap? */
    bson_reader_t *reader;
    const bson_t *b;
    bson_bool_t eof = FALSE;
    bson_off_t offset = 0;
    bson_size_t buffer_size;

    if (!PyArg_ParseTuple(args, "O", &array)) {
        goto error;
    }
    if (!array || !PyByteArray_Check(array)) {
        PyErr_SetString(PyExc_TypeError, "array must be a bytearray.");
        goto error;
    }
    ret = PyList_New(0);
    if (!ret) {
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
         * TODO: we should know length by now and pass it in, too.
         * Otherwise have a doc in half-initialized state.
         */
        doc = bson_doc_new(array, offset);
        if (!doc) {
            bson_reader_destroy(reader);
            goto error;
        }
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
    return NULL;
}

/*
 * TODO: can't we use METH_O?
 */
static PyMethodDef method = {
    "load_from_bytearray", (PyCFunction)load_from_bytearray, METH_VARARGS,
    "A BSONDocumentIterator over a bytearray of BSON documents."
};

int
init_bson_document_iterator(PyObject* module)
{
    PyObject *func = NULL;
    func = PyCFunction_New(&method, NULL);
    if (!func) {
        goto error;
    }
    if (PyModule_AddObject(module, "load_from_bytearray", func) < 0) {
        goto error;
    }

    BSONDocumentIterator_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&BSONDocumentIterator_Type) < 0) {
        goto error;
    }
    Py_INCREF(&BSONDocumentIterator_Type);
    if (PyModule_AddObject(module, "BSONDocumentIterator",
                           (PyObject *)&BSONDocumentIterator_Type) < 0) {
        goto error;
    }

    return 0;

error:
    Py_XDECREF(func);
    return -1;
}
