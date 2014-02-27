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

#ifndef DECODING_HELPERS_H
#define DECODING_HELPERS_H

#include "bson.h"

#include "bson_buffer.h"
#include "bson_document.h"

typedef struct {
    bson_t bson;
    bson_iter_t iter;
} bson_and_iter_t;

/*
 * Initialize a bson_t and bson_iter_t, or set exception and return FALSE.
 */
int
bson_doc_iter_init(BSONDocument *doc, bson_and_iter_t *bson_and_iter);

/*
 * Decode the value at the current position, or set exception and return NULL.
 */
PyObject *
bson_iter_py_value(bson_iter_t *iter, BSONBuffer *buffer);

#endif /* DECODING_HELPERS_H */
