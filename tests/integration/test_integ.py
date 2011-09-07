import pytest
import os, os.path
import shutil
import tempfile
import subprocess
import time
import socket

def pytest_funcarg__server(request):
    "Returns a new APIHandler with a filter manager"
    # Create tmpdir and delete after
    tmpdir = tempfile.mkdtemp()
    # Write the configuration
    config_path = os.path.join(tmpdir,"config.cfg")
    conf = """[bloomd]
data_dir = %(dir)s
port = 8210
""" % {"dir":tmpdir}
    open(config_path, "w").write(conf)

    # Start the process
    proc = subprocess.Popen("bloomd --file %s" % config_path,shell=True)
    proc.poll()
    assert proc.returncode is None

    # Define a cleanup handler
    def cleanup():
        try:
            proc.terminate()
            proc.wait()
            shutil.rmtree(tmpdir)
        except:
            pass
    request.addfinalizer(cleanup)

    # Make a connection to the server
    connected = False
    for x in xrange(3):
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.settimeout(1)
            conn.connect(("localhost",8210))
            connected = True
            break
        except Exception, e:
            print e
            time.sleep(0.3)

    # Die now
    if not connected:
        raise EnvironmentError, "Failed to connect!"

    # Return the connection
    return conn

class TestInteg(object):
    def test_list_empty(self, server):
        "Tests doing a list on a fresh server"
        fh = server.makefile()
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

    def test_create(self, server):
        "Tests creating a filter"
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"

    def test_doublecreate(self, server):
        "Tests creating a filter twice"
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("create foobar\n")
        assert fh.readline() == "Exists\n"

    def test_drop(self, server):
        "Tests dropping a filter"
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"
        server.sendall("drop foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

    def test_set(self, server):
        "Tests setting a value"
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_doubleset(self, server):
        "Tests setting a value"
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "No\n"

    def test_check(self, server):
        "Tests checking a value"
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("check foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_set_check(self, server):
        "Tests setting and checking many values"
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        for x in xrange(10000):
            server.sendall("set foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"
        for x in xrange(10000):
            server.sendall("check foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"

