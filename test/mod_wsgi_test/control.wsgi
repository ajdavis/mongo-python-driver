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

"""A WSGI app that doesn't use bson. A control group for tests of
mod_wsgi_test_runtime_warning.wsgi.
"""

import os
import sys

try:
    from mod_wsgi import version as mod_wsgi_version
except ImportError:
    mod_wsgi_version = None


import warnings


def application(environ, start_response):
    warnings.simplefilter('always')
    pid = os.getpid()
    output = 'python %s, mod_wsgi %s\nPID %s\n' % (
        sys.version, mod_wsgi_version, pid)

    response_headers = [('Content-Length', str(len(output)))]
    start_response('200 OK', response_headers)
    return [output]
