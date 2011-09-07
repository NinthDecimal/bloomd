"""
A lightweight benchmarking script for bloomd, to check for performance improvements.
"""
from functools import wraps
import time
import socket
import random

HOST="localhost"
PORT=8673
UDP_PORT=8674
FILTER="foobar"
NUM=100000

def timeit(func):
    @wraps(func)
    def wrapper(*args,**kwargs):
        start = time.time()
        res = func(*args,**kwargs)
        end = time.time()
        print "Time:",func.func_name,end-start,"secs"
        wrapper.timing = end-start
        return res
    return wrapper

def connection_tcp():
    "Returns a socket connection to the server"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST,PORT))
    return s

def connection_udp():
    "Returns a udp socket connection to the server"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((HOST,UDP_PORT))
    return s

@timeit
def create_filter(conn, fh):
    "Creates a new filter"
    global FILTER
    num = random.randint(1,100000)
    conn.sendall("create test%d\n" %num)
    FILTER = "test%d" % num
    return "Done" in fh.readline()

@timeit
def create_filter_udp(conn_udp):
    "Creates a new filter"
    global FILTER
    num = random.randint(1,100000)
    conn_udp.sendall("create test%d\n" %num)
    FILTER = "test%d" % num
    return True

@timeit
def sets(conn, fh, num=NUM):
    "Runs a series of set operations"
    for x in xrange(num):
        conn.sendall("set %s %s\n" % (FILTER, "test%d" % x))

    yes_count = 0
    for x in xrange(num):
        line = fh.readline()
        if "Yes" in line: yes_count+=1
    return yes_count

@timeit
def sets_udp(conn_udp, num=NUM):
    "Runs a series of set operations"
    for x in xrange(num):
        conn_udp.sendall("set %s %s\n" % (FILTER, "test%d" % x))
    return NUM

@timeit
def checks(conn, fh, num=NUM):
    "Runs a series of check operations"
    for x in xrange(num):
        conn.sendall("check %s %s\n" % (FILTER, "test%d" % x))

    yes_count = 0
    for x in xrange(num):
        line = fh.readline()
        if "Yes" in line: yes_count+=1
    return yes_count

def main():
    conn = connection_tcp()
    conn_udp = connection_udp()
    fh = conn.makefile()
    print "Create:",create_filter(conn, fh), FILTER
    print "Sets:",sets(conn, fh)
    print "Sets/sec",NUM/sets.timing
    print "Checks:",checks(conn, fh)
    print "Checks/sec",NUM/checks.timing

    # Do the UDP stuff
    print "Create UDP:",create_filter_udp(conn_udp), FILTER
    print "Sets UDP:",sets_udp(conn_udp)
    print "Sets UDP/sec",NUM/sets_udp.timing
    print "Checks:",checks(conn, fh)
    print "Checks/sec",NUM/checks.timing

if __name__ == "__main__":
    main()

