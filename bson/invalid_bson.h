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


#ifndef INVALID_BSON_H
#define INVALID_BSON_H

/*
 * Set the current exception to InvalidBSON, optionally with a message.
 * msg can be NULL.
 * Call this before returning NULL from a BSON decoder function.
 */
void
raise_invalid_bson(const char *msg);

#endif /* INVALID_BSON_H */
