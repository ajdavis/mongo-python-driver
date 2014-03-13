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

#include "bson_buffer.h"

#define IS_INFLATED(doc) (!doc->buffer)

/*
 * When first created, a document is a pointer into the BSON buffer. If it
 * is inflated (either from frequent lookups by key, or because the buffer
 * is deallocated), it fills out the dict and dereferences the buffer.
 *
 * All documents for a single buffer are stored in a doubly-linked list so
 * they can be notified when the buffer is being deallocated. Their position
 * in the list is unrelated to their offset in the buffer.
 */
typedef struct PyBSONDocument {
    /* Superclass. */
    PyDictObject dict;
    /* Buffer from which we're reading. Not reference-counted. */
    PyBSONBuffer *buffer;
    /* This document's offset into array. */
    off_t offset;
    /* This document's length. */
    int length;
    /* How many times were we accessed before inflating? */
    char n_accesses;
    /* Neighbors in list of documents, all pointing to the same buffer. */
    struct PyBSONDocument *prev, *next;
    /* Preserve key order after we inflate. */
    PyObject *keys;
} PyBSONDocument;

/*
 * Replace linear access with a hash table.
 * Does not release the buffer. Returns 1 on success.
 */
int
bson_doc_inflate(PyBSONDocument *doc);

/*
 * Call inflate() and release the buffer. Returns 1 on success.
 */
int
bson_doc_detach(PyBSONDocument *doc);

/*
 * Create a PyBSONDocument.
 */
PyBSONDocument *
PyBSONDocument_New(PyBSONBuffer *buffer, off_t start, off_t end);

/*
 * Add PyBSONDocument and related functions to module.
 */
int
init_bson_document(PyObject* module);

#endif /* BSON_DOCUMENT_H */
