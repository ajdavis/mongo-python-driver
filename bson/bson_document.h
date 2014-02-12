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

#ifndef BSON_DOCUMENT_H
#define BSON_DOCUMENT_H

#include "bson.h"

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

/*
 * TODO: move?
 */
PyObject *
bson_iter_py_value(bson_iter_t *iter);

/*
 * Create a BSONDocument from a bytearray and offsets.
 */
PyObject *
BSONDocument_New(PyObject *array, bson_off_t start, bson_off_t end);

/*
 * Add BSONDocument and related functions to module.
 */
int
init_bson_document(PyObject* module);

#endif /* BSON_DOCUMENT_H */
