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

#include "bson_buffer.h"
#include "bson_buffer_iter.h"
#include "bson_document.h"
#include "invalid_bson.h"

PyObject *
PyBSONBufferIter_IterNext(PyBSONBufferIter *iter) {
    off_t start;
    off_t end;
    unsigned int eof = 0;
    PyBSONDocument *doc = NULL;
    bson_reader_t *reader = iter->reader;

    if (!iter->valid) {
        raise_invalid_bson_str("Buffer contains invalid BSON");
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
        reader = iter->reader = NULL;
        if (eof) {
            /*
             * Normal completion.
             */
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        } else {
            raise_invalid_bson_str("Buffer contains invalid BSON");
            goto error;
        }
    }

    end = bson_reader_tell(reader);
    doc = PyBSONDocument_New(iter->buffer, start, end);
    if (!doc)
        goto error;

    bson_buffer_attach_doc(iter->buffer, doc);
    return (PyObject *)doc;

error:
    /*
     * Invalidate the iterator.
     */
    if (reader) {
        bson_reader_destroy(reader);
    }

    iter->reader = NULL;
    iter->valid = 0;
    Py_XDECREF(doc);
    return NULL;
}

static void
BSONBufferIter_Dealloc(PyBSONBufferIter* iter)
{
    if (iter->reader)
        bson_reader_destroy(iter->reader);

    Py_XDECREF(iter->buffer);
    PyObject_Del((PyObject*)iter);
}

static PyTypeObject PyBSONBufferIter_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "bson.BSONBufferIterator", /*tp_name*/
    sizeof(PyBSONBufferIter),  /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)BSONBufferIter_Dealloc, /*tp_dealloc*/
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
    "BSONBuffer iterator.",    /* tp_doc */
    0,                         /*tp_traverse */
    0,                         /*tp_clear */
    0,                         /*tp_richcompare */
    0,                         /*tp_weaklistoffset */
    PyObject_SelfIter,         /*tp_iter: __iter__() method */
    (iternextfunc)PyBSONBufferIter_IterNext, /* tp_iternext: next() method */
    0,                         /*tp_methods */
    0,                         /*tp_members */
    0,                         /*tp_getset */
    0,                         /*tp_base */
    0,                         /*tp_dict */
    0,                         /*tp_descr_get */
    0,                         /*tp_descr_set */
    0,                         /*tp_dictoffset */
    0,                         /*tp_init: this isn't created from Python */
    0,                         /*tp_alloc */
    0,                         /*tp_new */
};

PyBSONBufferIter *
bson_buffer_iter_new(PyBSONBuffer *buffer)
{
    bson_reader_t *reader = NULL;
    size_t buffer_size;

    PyBSONBufferIter *iter = PyObject_New(PyBSONBufferIter,
                                          &PyBSONBufferIter_Type);
    if (!iter)
        return NULL;

    /* No initproc, we do it all here. */
    iter->buffer = buffer;
    Py_INCREF(buffer);
    /* TODO: move to a function: */
    buffer_size = PyByteArray_Size(buffer->array);
    iter->reader = bson_reader_new_from_data(
            (uint8_t *)PyByteArray_AsString(buffer->array),
            buffer_size);

    if (!iter->reader)
        goto error;

    iter->valid = 1;
    return iter;

error:
    Py_CLEAR(iter->buffer);
    if (iter->reader)
        bson_reader_destroy(reader);

    return NULL;
}

int
init_bson_buffer_iter(PyObject* module)
{
    PyBSONBufferIter_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&PyBSONBufferIter_Type) < 0)
        return -1;

    return 0;
}
