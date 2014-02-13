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

#ifndef BSON_BUFFER_H
#define BSON_BUFFER_H

struct BSONDocument; // forward declaration.

typedef struct {
    PyObject_HEAD
    /* Doubly-linked list of active BSONDocuments referring to this buffer. */
    struct BSONDocument *dependents;
    /* A bytearray. */
    PyObject *array;
    bson_reader_t *reader;
} BSONBuffer;

void
bson_buffer_attach_doc(BSONBuffer *buffer, struct BSONDocument *doc);

/*
 * Add BSONBuffer and related functions to module.
 */
int
init_bson_buffer(PyObject* module);

#endif /* BSON_BUFFER_H */
