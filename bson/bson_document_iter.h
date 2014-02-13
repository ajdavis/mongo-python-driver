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

#ifndef BSON_DOCUMENT_ITER_H
#define BSON_DOCUMENT_ITER_H

#include "bson_document.h"

/*
 * TODO: iterkeys and itervalues.
 */

//PyObject *
//bson_doc_iterkeys(BSONDocument *doc);
//
//PyObject *
//bson_doc_itervalues(BSONDocument *doc);

PyObject *
BSONDocument_IterItems(BSONDocument *doc);

int
init_bson_document_iter(PyObject *module);

#endif /* BSON_DOCUMENT_ITER_H */
