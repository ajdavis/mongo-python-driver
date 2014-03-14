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

#include "invalid_bson.h"
#include "utlist.h"
#include "bson_document.h"
#include "bson_buffer.h"
#include "bson_buffer_iter.h"

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

/*
 * Common functionality: Initialize a PyBSONBuffer,
 * or set exception and return false.
 */
static int
bson_buffer_init(PyBSONBuffer *buffer, PyObject *data, PyObject *as_class,
                 int tz_aware, int uuid_subtype, int compile_re)
{
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

    buffer->array = array;
    buffer->dependents = NULL;
    buffer->as_class = as_class;
    Py_XINCREF(as_class);
    buffer->tz_aware = tz_aware;
    buffer->uuid_subtype = uuid_subtype;
    buffer->compile_re = compile_re;
    return 1;

error:
    buffer->array = NULL;
    Py_XDECREF(array);
    buffer->dependents = NULL;
    return 0;
}

static int
bson_buffer_initproc(PyBSONBuffer *self, PyObject *args, PyObject *kwds)
{
    /*
     * TODO: refactor arg parsing with _cbson_bson_to_dict().
     */
    PyObject *data = NULL;
    PyObject* as_class = (PyObject*)&PyDict_Type;
    unsigned char tz_aware = 1;
    unsigned char uuid_subtype = 3;
    unsigned char compile_re = 1;

    if (!PyArg_ParseTuple(
            args, "O|Obbb",
            &data, &as_class, &tz_aware, &uuid_subtype, &compile_re)) {
        goto error;
    }

    if (!bson_buffer_init(self, data, as_class, tz_aware, uuid_subtype,
                          compile_re))
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
    Py_TPFLAGS_DEFAULT,        /*tp_flags */
    "Buffer of BSON bytes.",   /*tp_doc */
    0,                         /*tp_traverse */
    0,                         /*tp_clear */
    0,                         /*tp_richcompare */
    0,                         /*tp_weaklistoffset */
    (getiterfunc)bson_buffer_iter_new, /*tp_iter: __iter__() method*/
    0,                         /*tp_iternext*/
    0,                         /*tp_methods*/
    0,                         /*tp_members*/
    0,                         /*tp_getset*/
    0,                         /*tp_base*/
    0,                         /*tp_dict*/
    0,                         /*tp_descr_get*/
    0,                         /*tp_descr_set*/
    0,                         /*tp_dictoffset*/
    (initproc)bson_buffer_initproc, /*tp_init*/
    0,                         /*tp_alloc*/
    0,                         /*tp_new*/
};

PyBSONBuffer *
bson_buffer_new(PyObject *data, PyObject *as_class, int tz_aware,
                int uuid_subtype, int compile_re)
{
    PyBSONBuffer *buffer = PyObject_New(PyBSONBuffer, &PyBSONBuffer_Type);
    if (!buffer)
        goto error;

    if (!bson_buffer_init(buffer, data, as_class, tz_aware, uuid_subtype,
                          compile_re))
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
