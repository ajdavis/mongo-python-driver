import sys
import nose
from nose.config import Config
from os import path
import re
import fake_pymongo

if __name__ == '__main__':
    # Monkey-patch all pymongo's unittests so they think our fake pymongo is the
    # real one
    sys.modules['pymongo'] = fake_pymongo

    for submod in [
        'connection',
        'collection',
        'master_slave_connection',
        'replica_set_connection',
        'database',
    ]:
        # So that 'from pymongo.connection import Connection' gets the fake
        # Connection, not the real
        sys.modules['pymongo.%s' % submod] = fake_pymongo

    # Find our directory
    this_dir = path.dirname(__file__)

    # Find test dir
    test_dir = path.normpath(path.join(this_dir, '../../../test'))
    print 'Running tests in %s' % test_dir

    # Exclude a test that hangs and prevents the run from completing - we should
    # fix the test for async, eventually
    # TODO: fix test_multiprocessing
    print 'WARNING: excluding test_multiprocessing, which would hang'
    config = Config(
        exclude=[
            re.compile(r'test_multiprocessing'),
            re.compile(r'test_ensure_unique_index_threaded'),
        ]
    )

    nose.run(defaultTest=this_dir, config=config)
