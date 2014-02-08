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

typedef struct {
	PyDictObject dict;
	void *buffer;
	long offset;
} NoDict;

// TODO: needed?
static PyMethodDef NoDict_methods[] = {
    {NULL,	NULL},
};

static int
NoDict_init(NoDict *self, PyObject *args, PyObject *kwds)
{
    printf("nodict_init\n");
	// TODO: shouldn't expect any args?
    if (PyDict_Type.tp_init((PyObject *)self, args, kwds) < 0) {
        return -1;
    }
    self->buffer = NULL;
    self->offset = 0;
    return 0;
}

static void
NoDict_dealloc(NoDict *self)
{
    // TODO???
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
    PyDict_Type.tp_init,//(initproc)NoDict_init,   /* tp_init */
    0,                       /* tp_alloc */
    0,                       /* tp_new */
};

PyObject *
_cbson_load_from_memoryview(PyObject *self, PyObject *args) {
	PyObject *view = NULL;
	NoDict *nodict = NULL;
	if (!PyArg_ParseTuple(args, "O", &view))
		return NULL;

    // TODO: check if memoryview

//	nodict = PyObject_New(NoDict, &NoDict_Type);
	nodict = (NoDict*)PyType_GenericNew(&NoDict_Type, NULL, NULL);
//    nodict = (NoDict*)PyObject_Init((PyObject*)nodict, &NoDict_Type);
    printf("nodict %x\n", nodict);

	return (PyObject*)nodict;
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
