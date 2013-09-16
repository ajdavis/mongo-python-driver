# Copyright 2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reproduce RuntimeError in BSON.encode after mod_wsgi creates a new sub
interpreter.
"""

import os
import re
import sys
import uuid

this_path = os.path.dirname(os.path.join(os.getcwd(), __file__))

# Location of PyMongo checkout
repository_path = os.path.normpath(os.path.join(this_path, '..', '..'))
sys.path.insert(0, repository_path)

import bson
assert bson.has_c()

import pymongo

try:
    from mod_wsgi import version as mod_wsgi_version
except ImportError:
    mod_wsgi_version = None


import warnings


def application(environ, start_response):
    warnings.simplefilter('always')
    for i in range(1000):
        bson.BSON.encode({
            'oid': bson.ObjectId(),
            'ts': bson.Timestamp(0, 0),
            're': re.compile(''),
            'u': uuid.uuid4(),
        })

    pid = os.getpid()
    output = 'python %s, mod_wsgi %s, pymongo %s\nat %s\nPID %s\n' % (
        sys.version, mod_wsgi_version, pymongo.version, repository_path,
        pid)

    response_headers = [('Content-Length', str(len(output)))]
    start_response('200 OK', response_headers)
    return [output]
