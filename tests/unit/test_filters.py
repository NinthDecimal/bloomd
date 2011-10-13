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
    return dict(config.DEFAULTS)

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

def pytest_funcarg__manager(request):
    "Returns a fake FilterManager object"
    class Manager(object):
        def __init__(self):
            self.filters = {}
    return Manager()

class TestFilter(object):
    def test_filter_blank(self, config, tmpdir):
        "Tests creating a blank filter"
        filter = filter_manager.Filter(config, "test", tmpdir)
        assert len(filter) == 0
        assert len(filter.filter.filters) == 1

    def test_filter_blank_discover(self, config, tmpdir):
        "Tests creating a blank filter"
        filter = filter_manager.Filter(config, "test", tmpdir, discover=True)
        assert len(filter) == 0
        assert len(filter.filter.filters) == 1

    def test_filter_blank_custom(self, config, tmpdir):
        "Tests creating a blank filter with custom parameters"
        custom = {"initial_capacity":1000,
                  "default_probability":0.001,
                  "scale_size":3,
                  "probability_reduction":0.5
                 }
        filter = filter_manager.Filter(config, "test", tmpdir, custom=custom)
        assert len(filter) == 0
        assert len(filter.filter.filters) == 1
        assert filter.capacity() == 1000
        assert filter.filter.scale_size == 3
        assert filter.filter.prob_reduction == 0.5
        assert filter.filter.prob == 0.001

    def test_filter_doubleflush(self, config, tmpdir):
        "Tests double flushing a filter"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter) == 1000
        filter.flush()
        filter.flush()

    def test_filter_flushclose(self, config, tmpdir):
        "Tests a flush followed by a close"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter) == 1000
        filter.flush()
        filter.close()

    def test_filter_flush(self, config, tmpdir):
        "Tests a flush works"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter) == 1000
        filter.flush()

        filter2 = filter_manager.Filter(config, "test2", tmpdir, discover=True)
        [filter2.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter2) == 1000
        assert all([filter.__contains__("Test%d"%x) for x in xrange(1000)])

        filter.close()
        filter2.close()

    def test_filter_close_does_flush(self, config, tmpdir):
        "Tests closing a filter flushes it"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter) == 1000
        filter.close()

        filter = filter_manager.Filter(config, "test2", tmpdir, discover=True)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter) == 1000
        assert all([filter.__contains__("Test%d"%x) for x in xrange(1000)])
        filter.close()

    def test_filter_double_add(self, config, tmpdir):
        "Tests keys cannot be double added"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        assert len(filter) == 1000
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

class TestProxyFilter(object):
    def test_filter_blank(self, config, tmpdir, manager):
        "Tests creating a blank proxy"
        filter = filter_manager.ProxyFilter(manager, config, "test", tmpdir)
        assert len(filter) == 0
        assert filter.capacity() == config["initial_capacity"]

    def test_filter_close(self, config, tmpdir, manager):
        "Tests a close"
        filter = filter_manager.ProxyFilter(manager, config, "test", tmpdir)
        filter.close()

    def test_filter_flush(self, config, tmpdir, manager):
        "Tests a flush does not crash"
        filter = filter_manager.ProxyFilter(manager, config, "test", tmpdir)
        filter.flush()

    def test_filter_bytes_match(self, config, tmpdir, manager):
        "Tests that the byte sizes match between a proxy and real filter"
        filter = filter_manager.Filter(config, "test", tmpdir)
        filter.flush()
        proxy = filter_manager.ProxyFilter(manager, config, "test", tmpdir,
                                           custom=filter_manager.load_custom_settings(tmpdir))
        assert proxy.byte_size() == filter.byte_size()

    def test_filter_delete(self, config, tmpdir, manager):
        "Tests deleting a filter"
        filter = filter_manager.Filter(config, "test", tmpdir)
        [filter.add("Test%d" %x) for x in xrange(1000)]
        filter.close()
        proxy = filter_manager.ProxyFilter(manager, config, "test", tmpdir)
        proxy.delete()
        assert not os.path.exists(tmpdir)

    def test_filter_add_faults(self, config, tmpdir, manager):
        "Tests that calling add faults, but succeeds"
        proxy = filter_manager.ProxyFilter(manager, config, "test", tmpdir)
        manager.filters["test"] = proxy
        [manager.filters["test"].add("Test%d" % x) for x in xrange(1000)]

        assert isinstance(manager.filters["test"], filter_manager.Filter)
        assert all([("Test%d" % x) in manager.filters["test"] for x in xrange(1000)])

    def test_filter_contain_faults(self, config, tmpdir, manager):
        "Tests that calling contain faults, but succeeds"
        proxy = filter_manager.ProxyFilter(manager, config, "test", tmpdir)
        manager.filters["test"] = proxy
        contained = [("Test%d" % x) in manager.filters["test"] for x in xrange(1000)]

        assert isinstance(manager.filters["test"], filter_manager.Filter)
        assert not any(contained)


class TestFilterManager(object):
    def test_init(self, config, tmpdir):
        "Makes a filter manager with stock config, should work."
        config["data_dir"] = tmpdir
        f = filter_manager.FilterManager(config)
        assert len(f) == 0

    def test_create(self, config, tmpdir):
        "Makes a filter manager with stock config, should work."
        config["data_dir"] = tmpdir
        f = filter_manager.FilterManager(config)
        assert len(f) == 0
        foo = f.create_filter("foo")
        assert len(f) == 1
        assert "foo" in f
        assert f["foo"] == foo

    def test_delete(self, config, tmpdir):
        "Makes a filter manager with stock config, should work."
        config["data_dir"] = tmpdir
        f = filter_manager.FilterManager(config)
        assert len(f) == 0
        f.create_filter("foo")
        del f["foo"]
        assert len(f) == 0
        assert "foo" not in f
        assert os.listdir(tmpdir) == []

    def test_close(self, config, tmpdir):
        "Tests close closes things"
        config["data_dir"] = tmpdir
        f = filter_manager.FilterManager(config)
        assert len(f) == 0
        foo = f.create_filter("foo")
        assert foo.filter is not None
        f.close()
        assert foo.filter is None

    def test_flush(self, config, tmpdir):
        "Tests flush"
        config["data_dir"] = tmpdir
        f = filter_manager.FilterManager(config)
        foo = f.create_filter("foo")
        assert foo.dirty
        f._flush()
        assert not foo.dirty

    def test_discover(self, config, tmpdir):
        "Tests discovery of filters"
        config["data_dir"] = tmpdir

        # Make a filter
        filter_dir = os.path.join(tmpdir,filter_manager.FILTER_PREFIX+"test")
        os.mkdir(filter_dir)

        f = filter_manager.FilterManager(config)
        assert "test" in f
        assert f["test"].capacity() == config["initial_capacity"]

    def test_recovery(self, config, tmpdir):
        "Tests recovering existing filters"
        config["data_dir"] = tmpdir
        f = filter_manager.FilterManager(config)
        f.create_filter("foo")
        f.create_filter("bar")
        f.create_filter("baz")
        f.close()

        f = filter_manager.FilterManager(config)
        assert "foo" in f
        assert "bar" in f
        assert "baz" in f

