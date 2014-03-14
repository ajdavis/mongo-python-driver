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

#include <stdbool.h>

#include "Python.h"

#include "bson.h"

#include "invalid_bson.h"
#include "utlist.h"
#include "bson_document.h"
#include "bson_buffer.h"

/*
 * Add doc to list of dependents.
 */
void
bson_buffer_attach_doc(PyBSONBuffer *buffer, PyBSONDocument *doc)
{
    /*
     * Dependents aren't reference-counted. We manually ensure that
     * dealloc'ed docs are removed from dependents, and if the buffer
     * is dealloc'ed it detaches all the docs from itself.
     */
    DL_APPEND(buffer->dependents, doc);
}

PyObject *
PyBSONBuffer_IterNext(PyBSONBuffer *buffer) {
    off_t start;
    off_t end;
    unsigned int eof = 0;
    PyBSONDocument *doc = NULL;
    bson_reader_t *reader = buffer->reader;

    if (!buffer->valid) {
        raise_invalid_bson("Buffer contains invalid BSON");
        goto error;
    }
    if (!reader) {
        /*
         * Finished.
         */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    start = bson_reader_tell(reader);
    /*
     * TODO: useful? or just read the length prefix ?
     */
    if (!bson_reader_read(reader, &eof)) {
        /*
         * This was the last document, or there was an error.
         */
        bson_reader_destroy(reader);
        reader = buffer->reader = NULL;
        if (eof) {
            /*
             * Normal completion.
             */
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } else {
            raise_invalid_bson("Buffer contains invalid BSON");
            goto error;
        }
    }

    end = bson_reader_tell(reader);
    doc = PyBSONDocument_New(buffer, start, end);
    if (!doc)
        goto error;

    bson_buffer_attach_doc(buffer, doc);
    return (PyObject *)doc;

error:
    /*
     * Invalidate the iterator.
     */
    if (reader) {
        bson_reader_destroy(reader);
    }

    buffer->reader = NULL;
    buffer->valid = 0;
    Py_XDECREF(doc);
    return NULL;
}

/*
 * Common functionality: Initialize a PyBSONBuffer,
 * or set exception and return false.
 */
static int
bson_buffer_init(PyBSONBuffer *buffer, PyObject *data)
{
    /*
     * TODO: Does this have to be a separate allocation from the BSONBuffer?
     */
    bson_reader_t *reader = NULL;
    size_t buffer_size;
    PyObject *array = NULL;

    if (PyByteArray_Check(data)) {
        array = data;
        Py_INCREF(array);
    } else if (PyBytes_Check(data)) {
        array = PyByteArray_FromObject(data);
        if (!array)
            goto error;
    } else {
        PyErr_SetString(PyExc_TypeError, "data must be a bytearray or bytes.");
        goto error;
    }

    buffer_size = PyByteArray_Size(array);
    reader = bson_reader_new_from_data(
            (uint8_t *)PyByteArray_AsString(array),
            buffer_size);

    if (!reader)
        goto error;

    buffer->array = array;
    buffer->reader = reader;
    buffer->dependents = NULL;
    buffer->valid = 1;
    return 1;

error:
    buffer->array = NULL;
    Py_XDECREF(array);
    if (reader)
        bson_reader_destroy(reader);

    buffer->reader = NULL;
    buffer->dependents = NULL;
    buffer->valid = 0;
    return 0;
}

static int
bson_buffer_initproc(PyBSONBuffer *self, PyObject *args, PyObject *kwds)
{
    PyObject *data = NULL;
    if (!PyArg_ParseTuple(args, "O", &data))
        goto error;

    if (!bson_buffer_init(self, data))
        goto error;

    return 0;

error:
    Py_XDECREF(data);
    return -1;
}

static void
BSONBuffer_Dealloc(PyBSONBuffer* self)
{
    PyBSONDocument *doc;

    /*
     * Order of destruction matters. Note that bson_doc_inflate() doesn't
     * affect our list of dependents or our reference count.
     */
    DL_FOREACH(self->dependents, doc) {
        bson_doc_inflate(doc);
    }

    if (self->reader)
        bson_reader_destroy(self->reader);

    Py_XDECREF(self->array);
    self->ob_type->tp_free((PyObject*)self);
}

static PyTypeObject PyBSONBuffer_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "bson.BSONBuffer",         /*tp_name*/
    sizeof(PyBSONBuffer),        /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)BSONBuffer_Dealloc, /*tp_dealloc*/
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
    "PyBSONDocument lazy-loading iterator.", /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    PyObject_SelfIter,         /* tp_iter: __iter__() method */
    (iternextfunc)PyBSONBuffer_IterNext, /* tp_iternext: next() method */
    0,                         /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)bson_buffer_initproc, /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};

PyBSONBuffer *
bson_buffer_new(PyObject *data)
{
    PyBSONBuffer *buffer = PyObject_New(PyBSONBuffer, &PyBSONBuffer_Type);
    if (!buffer)
        goto error;

    if (!bson_buffer_init(buffer, data))
        goto error;

    return buffer;

error:
    Py_XDECREF(buffer);
    return NULL;
}

int
init_bson_buffer(PyObject* module)
{
    PyBSONBuffer_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&PyBSONBuffer_Type) < 0)
        return -1;

    Py_INCREF(&PyBSONBuffer_Type);
    if (PyModule_AddObject(module,
                           "BSONBuffer",
                           (PyObject *)&PyBSONBuffer_Type) < 0)
        return -1;

    return 0;
}
