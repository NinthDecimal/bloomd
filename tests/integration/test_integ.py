import pytest
import os, os.path
import shutil
import tempfile
import subprocess
import time
import threading
import socket

def pytest_funcarg__servers(request):
    "Returns a new APIHandler with a filter manager"
    # Create tmpdir and delete after
    tmpdir = tempfile.mkdtemp()
    # Write the configuration
    config_path = os.path.join(tmpdir,"config.cfg")
    conf = """[bloomd]
data_dir = %(dir)s
port = 8210
udp_port = 8211
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

    # Make a UDP connection
    conn_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    conn_udp.connect(("localhost", 8211))

    # Return the connection
    return conn, conn_udp

class TestInteg(object):
    def test_list_empty(self, servers):
        "Tests doing a list on a fresh server"
        server, _ = servers
        fh = server.makefile()
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

    def test_create(self, servers):
        "Tests creating a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"

    def test_doublecreate(self, servers):
        "Tests creating a filter twice"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("create foobar\n")
        assert fh.readline() == "Exists\n"

    def test_drop(self, servers):
        "Tests dropping a filter"
        server, _ = servers
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

    def test_close(self, servers):
        "Tests closing a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"
        server.sendall("close foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

    def test_set(self, servers):
        "Tests setting a value"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_bulk(self, servers):
        "Tests setting bulk values"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("multi foobar test blah\n")
        assert fh.readline() == "No No\n"
        server.sendall("bulk foobar test blah\n")
        assert fh.readline() == "Yes Yes\n"

    def test_doubleset(self, servers):
        "Tests setting a value"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "No\n"

    def test_check(self, servers):
        "Tests checking a value"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("check foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_multi(self, servers):
        "Tests checking multiple values"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("multi foobar test test1 test2\n")
        assert fh.readline() == "No No No\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("multi foobar test test1 test2\n")
        assert fh.readline() == "Yes No No\n"

    def test_aliases(self, servers):
        "Tests aliases"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("b foobar test test1 test2\n")
        assert fh.readline() == "Yes Yes Yes\n"
        server.sendall("s foobar test\n")
        assert fh.readline() == "No\n"
        server.sendall("m foobar test1 test2\n")
        assert fh.readline() == "Yes Yes\n"
        server.sendall("c foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_set_check(self, servers):
        "Tests setting and checking many values"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        for x in xrange(1000):
            server.sendall("set foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"
        for x in xrange(1000):
            server.sendall("check foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"

    def test_create_udp(self, servers):
        "Tests creating a collection using UDP"
        server, server_udp = servers
        time.sleep(0.2)
        server_udp.sendall("create zomg\n")
        time.sleep(0.2)
        server.sendall("list\n")
        fh = server.makefile()
        assert fh.readline() == "START\n"
        assert "zomg" in fh.readline()
        assert fh.readline() == "END\n"

    def test_set_check_udp(self, servers):
        "Tests setting and checking many values using UDP to set"
        server, server_udp = servers
        fh = server.makefile()
        server_udp.sendall("create pingpong\n")
        for x in xrange(100):
            server_udp.sendall("set pingpong test%d\n" % x)
        for x in xrange(100):
            server.sendall("check pingpong test%d\n" % x)
            assert fh.readline() == "Yes\n"

    def test_concurrent_drop(self, servers):
        "Tests setting values and do a concurrent drop on the DB"
        server, server_udp = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(10000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                if resp != "Yes\n":
                    assert resp == "Filter does not exist\n" and x > 100
                    return
                else:
                    assert resp == "Yes\n"
            assert False

        def drop():
            time.sleep(0.2)
            server_udp.sendall("drop pingpong\n")

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=drop)
        t.start()
        loopset()

    def test_concurrent_close(self, servers):
        "Tests setting values and do a concurrent close on the DB"
        server, server_udp = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(10000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                if resp != "Yes\n":
                    assert resp == "Filter does not exist\n" and x > 100
                    return
                else:
                    assert resp == "Yes\n"
            assert False

        def close():
            time.sleep(0.2)
            server_udp.sendall("close pingpong\n")

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=close)
        t.start()
        loopset()

    def test_concurrent_flush(self, servers):
        "Tests setting values and do a concurrent flush"
        server, server_udp = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(10000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                assert resp == "Yes\n"

        def flush():
            for x in xrange(3):
                time.sleep(0.1)
                server_udp.sendall("flush pingpong\n")

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=flush)
        t.start()
        loopset()

    def test_concurrent_create(self, servers):
        "Tests creating a filter with concurrent sets"
        server, server_udp = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(1000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                assert resp == "Yes\n"
            for r in xrange(3):
                for x in xrange(1000):
                    server.sendall("set pingpong%d test%d\n" % (r,x))
                    resp = fh.readline()
                    assert resp == "Yes\n"

        def create():
            for x in xrange(10):
                time.sleep(0.05)
                server_udp.sendall("create pingpong%d\n" % x)

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=create)
        t.start()
        loopset()

