"""
Contains tests for the Filter and FilterManager classes.
"""
import os
import shutil
import tempfile
import pytest
from bloomd import config, filter_manager

def pytest_funcarg__config(request):
    "Returns the default configuration"
    return config.DEFAULTS

def pytest_funcarg__tmpdir(request):
    "Returns a tmpdir and automatically cleans it up"
    tmpdir = tempfile.mkdtemp()
    def cleanup():
        try:
            shutil.rmtree(tmpdir)
        except:
            pass
    request.addfinalizer(cleanup)
    return tmpdir

class TestFilter(object):
    def test_filter_blank(self, config, tmpdir):
        "Tests creating a blank filter"
        filter = filter_manager.Filter(config, "test", tmpdir)
        assert len(filter.filter) == 0
        assert len(filter.filter.filters) == 1

    def test_filter_blank_discover(self, config, tmpdir):
        "Tests creating a blank filter"
        filter = filter_manager.Filter(config, "test", tmpdir, discover=True)
        assert len(filter.filter) == 0
        assert len(filter.filter.filters) == 1

    def test_filter_blank_custom(self, config, tmpdir):
        "Tests creating a blank filter with custom parameters"
        custom = {"initial_capacity":1000,
                  "default_probability":0.001,
                  "scale_size":3,
                  "probability_reduction":0.5
                 }
        filter = filter_manager.Filter(config, "test", tmpdir, custom=custom)
        assert len(filter.filter) == 0
        assert len(filter.filter.filters) == 1
        assert filter.filter.total_capacity() == 1000
        assert filter.filter.scale_size == 3
        assert filter.filter.prob_reduction == 0.5
        assert filter.filter.prob == 0.001

    def test_filter_doubleflush(self, config, tmpdir):
        "Tests double flushing a filter"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter.filter) == 1000
        filter.flush()
        filter.flush()

    def test_filter_flushclose(self, config, tmpdir):
        "Tests a flush followed by a close"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter.filter) == 1000
        filter.flush()
        filter.close()

    def test_filter_flush(self, config, tmpdir):
        "Tests a flush works"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter.filter) == 1000
        filter.flush()

        filter2 = filter_manager.Filter(config, "test2", tmpdir, discover=True)
        [filter2.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter2.filter) == 1000
        assert all([filter.__contains__("Test%d"%x) for x in xrange(1000)])

        filter.close()
        filter2.close()

    def test_filter_close_does_flush(self, config, tmpdir):
        "Tests closing a filter flushes it"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter.filter) == 1000
        filter.close()

        filter = filter_manager.Filter(config, "test2", tmpdir, discover=True)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter.filter) == 1000
        assert all([filter.__contains__("Test%d"%x) for x in xrange(1000)])
        filter.close()

    def test_filter_double_add(self, config, tmpdir):
        "Tests keys cannot be double added"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter.filter) == 1000
        assert not any([filter.add("Test%d" %x) for x in xrange(1000)])
        filter.close()

    def test_filter_delete(self, config, tmpdir):
        "Tests deleting a filter"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        filter.delete()
        assert not os.path.exists(tmpdir)

    def test_filter_filenames(self, config, tmpdir):
        "Tests that the filename generation works"
        custom = {"initial_capacity":1000}
        filter = filter_manager.Filter(config, "test", tmpdir, custom=custom)
        assert len(filter.filter.filters) == 1
        [filter.add("Test%d" %x) for x in xrange(2000)]
        assert len(filter.filter.filters) == 2
        filter.flush()
        assert len(os.listdir(tmpdir)) == 3 # 2 mmap files + config


