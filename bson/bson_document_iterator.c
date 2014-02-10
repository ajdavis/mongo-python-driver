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
    PyObject *array;
    bson_reader_t *reader;
} BSONDocumentIterator;

PyObject *
bson_doc_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}

PyObject *
bson_doc_iternext(PyObject *self) {
    bson_off_t start;
    bson_off_t end;
    bson_bool_t eof = FALSE;
    BSONDocument *doc = NULL;
    BSONDocumentIterator *iter = (BSONDocumentIterator *)self;
    bson_reader_t *reader = iter->reader;

    if (!reader) {
        /*
         * Finished.
         */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    start = bson_reader_tell(reader);
    if (!bson_reader_read(reader, &eof)) {
        /*
         * This was the last document, or there was an error.
         */
        bson_reader_destroy(reader);
        reader = iter->reader = NULL;
        Py_CLEAR(iter->array);
        if (eof) {
            /*
             * Normal completion.
             */
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } else {
            /*
             * Raise InvalidBSON exception.
             */
            PyObject* InvalidBSON;
            PyObject* errors = PyImport_ImportModule("bson.errors");
            if (!errors)
                goto error;
            InvalidBSON = PyObject_GetAttrString(errors, "InvalidBSON");
            Py_DECREF(errors);
            if (!InvalidBSON)
                goto error;
            PyErr_SetString(InvalidBSON, "Buffer contained invalid BSON.");
            goto error;
        }
    }

    end = bson_reader_tell(reader);
    doc = bson_doc_new(iter->array, start, end);
    if (!doc)
        goto error;

    return (PyObject *)doc;

error:
    /*
     * Invalidate the iterator.
     * TODO: make future uses raise a different error than StopIteration.
     */
    if (reader) {
        bson_reader_destroy(reader);
    }

    iter->reader = NULL;
    Py_XDECREF(doc);
    return NULL;
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
bson_doc_iter_new(PyObject *array)
{
    BSONDocumentIterator *ret = NULL;
    bson_size_t buffer_size;
    /*
     * TODO: must this be a separate allocation from the BSONDocumentIterator?
     */
    bson_reader_t *reader = NULL;

    if (!array || !PyByteArray_Check(array)) {
        PyErr_SetString(PyExc_TypeError, "array must be a bytearray.");
        goto error;
    }

    buffer_size = PyByteArray_Size(array);
    reader = bson_reader_new_from_data(
            (bson_uint8_t *)PyByteArray_AsString(array),
            buffer_size);

    if (!reader) {
        goto error;
    }

    ret = PyObject_New(BSONDocumentIterator,
                       &BSONDocumentIterator_Type);
    if (!ret)
        goto error;

    Py_INCREF(array);
    ret->array = array;
    ret->reader = reader;
    return (PyObject *)ret;

error:
    Py_XDECREF(ret);
    if (reader) {
        bson_reader_destroy(reader);
    }

    return NULL;
}

/*
 * TODO: remove this, should just be BSONDocumentIterator(array) in Python.
 */
PyObject *
load_from_bytearray(PyObject *self, PyObject *array) {
    return bson_doc_iter_new(array);
}

static PyMethodDef method = {
    "load_from_bytearray", (PyCFunction)load_from_bytearray, METH_O,
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
