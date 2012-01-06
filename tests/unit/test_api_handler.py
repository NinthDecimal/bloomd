import tempfile
import os
import shutil
import pytest
from bloomd import config, conn_handler, filter_manager

def pytest_funcarg__handler(request):
    "Returns a new APIHandler with a filter manager"
    tmpdir = tempfile.mkdtemp()
    conf = dict(config.DEFAULTS)
    conf["data_dir"] = tmpdir
    manager = filter_manager.FilterManager(conf)
    conn_handler.APIHandler.MANAGER = manager

    def cleanup():
        try:
            manager.close()
            shutil.rmtree(tmpdir)
        except:
            pass
    request.addfinalizer(cleanup)

    return conn_handler.APIHandler

class TestAPI(object):
    def test_list_empty(self, handler):
        "Tries to do an empty list"
        assert handler.list() == {}

    def test_create(self, handler):
        "Tries to create a filter, default params"
        assert handler.create("test") == "Done"
        assert "test" in handler.list()

    def test_create_exists(self, handler):
        "Tries to create a filter already exists"
        assert handler.create("test") == "Done"
        assert "test" in handler.list()
        assert handler.create("test") == "Exists"

    def test_create_custom(self, handler):
        "Tries to create a filter, custom params"
        assert handler.create("test", "1000 0.001") == "Done"
        assert "test" in handler.list()
        stats = handler.info("test")
        assert "probability" in stats and stats["probability"] == "0.001"
        assert "capacity" in stats and stats["capacity"] == "1000"
        assert "size" in stats and stats["size"] == "0"

    def test_create_custom_invalid(self, handler):
        "Tries to create a filter, custom params that are invalid"
        assert "Client Error" in handler.create("test", "0 1")

    def test_create_in_memory(self, handler):
        "Tries to create a filter, specify in_memory"
        assert handler.create("test", "in_memory") == "Done"

    def test_create_on_disk(self, handler):
        "Tries to create a filter, specify on_disk"
        assert handler.create("test", "on_disk") == "Done"

    def test_delete(self, handler):
        "Tries to delete a filter"
        assert handler.create("test") == "Done"
        assert "test" in handler.list()
        assert handler.drop("test") == "Done"
        assert "test" not in handler.list()

    def test_delete_notexist(self, handler):
        "Tries to delete a filter that does not exist"
        assert handler.drop("test") == "Filter does not exist"

    def test_close(self, handler):
        "Tries to close a filter"
        assert handler.create("test") == "Done"
        assert "test" in handler.list()
        assert handler.close("test") == "Done"
        assert "test" not in handler.list()

    def test_close_notexist(self, handler):
        "Tries to close a filter that does not exist"
        assert handler.close("test") == "Filter does not exist"

    def test_check_nofilter(self, handler):
        "Checks for a key in a bad filter"
        assert handler.check("test","foo") == "Filter does not exist"

    def test_check_blankfilter(self, handler):
        "Checks for a key in a blank filter"
        handler.create("test")
        assert handler.check("test","foo") == "No"

    def test_check(self, handler):
        "Checks for a key in a blank filter"
        handler.create("test")
        assert handler.set("test","foo") == "Yes"
        assert handler.check("test","foo") == "Yes"

    def test_set(self, handler):
        "Checks for a key in a blank filter"
        handler.create("test")
        assert handler.set("test","foo") == "Yes"

    def test_bulk(self, handler):
        "Test bulk set of items in blank filter"
        handler.create("test")
        assert handler.bulk("test","foo bar") == "Yes Yes"
        assert handler.check("test","foo") == "Yes"
        assert handler.check("test","bar") == "Yes"
        assert handler.multi("test","foo bar") == "Yes Yes"

    def test_bulk_single(self, handler):
        "Test bulk set of items in blank filter"
        handler.create("test")
        assert handler.bulk("test","foobar") == "Yes"
        assert handler.check("test","foobar") == "Yes"
        assert handler.multi("test","foobar") == "Yes"

    def test_multi(self, handler):
        "Test multi get of items in blank filter"
        handler.create("test")
        assert handler.multi("test","foo bar") == "No No"
        assert handler.set("test","foo") == "Yes"
        assert handler.multi("test","foo bar") == "Yes No"

    def test_bulk_single(self, handler):
        "Test bulk set of items in blank filter"
        handler.create("test")
        assert handler.multi("test","foobar") == "No"
        assert handler.set("test","foobar") == "Yes"
        assert handler.multi("test","foobar") == "Yes"

    def test_doubleset(self, handler):
        "Test double set"
        handler.create("test")
        assert handler.set("test","foo") == "Yes"
        assert handler.set("test","foo") == "No"

    def test_set_notexist(self, handler):
        "Test set with no filter"
        assert handler.set("test","foo") == "Filter does not exist"

    def test_info_notexist(self, handler):
        "Tests info bad filter"
        assert handler.info("test") == "Filter does not exist"

    def test_info(self, handler):
        "Tests info is correct"
        handler.create("test","1000 0.001")
        for x in xrange(100):
            assert handler.set("test","test%d" %x) == "Yes"
        info = handler.info("test")
        assert "probability" in info and info["probability"] == "0.001"
        assert "capacity" in info and info["capacity"] == "1000"
        assert "size" in info and info["size"] == "100"

    def test_flush_notexist(self, handler):
        "Tests flushing on bad filter"
        assert handler.flush("test") == "Filter does not exist"

    def test_flush(self, handler):
        "Tests flushing"
        handler.create("test")
        assert handler.flush("test") == "Done"

    def test_flush_all(self, handler):
        "Tests flushing all filters"
        assert handler.flush() == "Done"

    def test_conf_all(self, handler):
        "Tests the config of the server"
        conf = handler.conf()
        assert conf == handler.MANAGER.config

    def test_conf_notexist(self, handler):
        "Tests the config of non existing filter"
        assert handler.conf("test") == "Filter does not exist"

    def test_conf(self, handler):
        "Tests the config of non existing filter"
        handler.create("test","1000 0.001")
        conf = handler.conf("test")
        assert conf["initial_capacity"] == 1000
        assert conf["default_probability"] == 0.001

