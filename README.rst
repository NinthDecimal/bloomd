BloomD
=========

BloomD exposes a simple ASCII protocol that is memcached like
for managing collections of bloom filters. Clients can create
bloom filters, which support check and set operations.

Features
--------

* Supports multiple scalable bloom filters
    - Starts small, grows to fit data
* Periodically flushes filters to disk for persistence
* Provides simple ASCII interface
    - Create / List / Drop filters 
    - Check / Set values in filters
    - Flush filters
    - Get configuration
    - Get filter stats
* Provides UDP interface
    - Request only, no response
    - Cheap filter 'set' operations without TCP setup and teardown
* Dead simple to start and administer

Install
-------

Download and install from source::
    
    python setup.py install

Usage
-----

Bloomd must be configured using a file which is in conf format.
Here is an example configuration file:

::

    # Settings for bloomd
    [bloomd]
    port = 8673
    udp_port = 8674
    data_dir = /mnt/bloomd
    log_dir = /var/log/bloomd.log
    log_level = INFO


Then run bloomd, pointing it to that file (assuming `/etc` right now)::

    bloomd -f /etc/bloomd.conf

Protocol
--------

By default, Bloomd will listen for TCP connections on port 8673.
It uses a simple ASCII protocol that is very similar to memcached.

A command has the following syntax::

    cmd [args][\r]\n

We start each line by specifying a command, providing optional arguments,
and ending the line in a newline (carriage return is optional).

There are a total of 8 commands:
* create - Create a new filter (a filter is a named bloom filter)
* list - List all filters 
* drop - Drop a filters
* check - Check if a key is in a filter 
* set - Set an item in a filter
* info - Gets info about a filter
* flush - Flushes all filters or just a specified one
* conf - Returns the default configuration, or the configuration of a single filter

For the ``create`` command, the format is::

    create filter_name [initial_capacity] [max_prob]

Where ``filter_name`` is the name of the filter,
and can contain the characters a-z, A-Z, 0-9, ., _.
If an initial capacity is provided the filter
will be created to store at least that many items in the initial filter.
Otherwise the configured default value will be used. 
If a maximum false positive probability is provided,
that will be used, otherwise the configured default is used.

As an example::

    create foobar 1000000 0.001

This will create a filter foobar that has a 1M initial capacity,
and a 1/1000 probability of generating false positives. Valid responses
are either "Done", or "Exists".

The ``list`` command takes no arguments, and returns information
about all the filters. Here is an example response::

    START
    foobar 0.001 1797211 1000000 0
    END 

This indicates a single filter named foobar, with a probability
of 0.001 of false positives, a 1.79MB size, a current capacity of
1M items, and 0 current items. The size and capacity automatically
scale as more items are added.

The ``drop`` command is like create, but only takes a filter name.
It can either return "Done" or "Filter does not exist".

Check and set look similar, they are either::

    [check|set] filter_name key

The command must specify a filter and a key to use.
They will either return "Yes", "No" or "Filter does not exist".

The ``info`` command takes a filter name, and returns
information about the filter. Here is an example output::

    START
    capacity 1000000
    checks 0
    check_hits 0
    check_misses 0
    probability 0.001
    sets 0
    set_hits 0
    set_misses 0
    size 0
    storage 1797211
    END

The command may also return "Filter does not exist" if the filter does
not exist.

The ``flush`` command may be called without any arguments, which
causes all filters to be flushed. If a filter name is provided
then that filter will be flushed. This will either return "Done" or
"Filter does not exist".

The final command ``conf`` is used to query the server configuration
or the filter configuration. filters may have some custom parameters
when they are created, and store the configurations with them. They hold
some configuration which is not directly relevant to a filter.

::

    conf [filter_name]

An example output is::

    conf
    START
    scale_size 4
    default_probability 1e-04
    data_dir /tmp/bloomd
    probability_reduction 0.9
    initial_capacity 1000000 
    flush_interval 60
    log_level DEBUG
    log_file /tmp/bloomd/bloomd.log
    port 8673
    udp_port 8674
    END


UDP Protocol
--------

In addition to the TCP protocol, Bloomd also provides a UDP interface
to avoid the overhead of establishing TCP connections. By default, 
Bloomd will listen for UDP connections on port 8674. The commands are the
exact same as the TCP version.

Each UDP packet may contain multiple commands separated by a newline,
and each packet may be up to 64K in size. It is important to note
that the Bloomd server will never respond to UDP requests with a result.

This means, the UDP interface is unsuitable for querying filters, but
is fine for creating and flushing filters, or seting new keys in the
filters.

Because packet loss may occur and UDP is not a reliable transport mechanism,
UDP should not be relied on if sets must occur reliably. Under heavy load,
the packets will be dropped and operations will fail to take place. In these
situations, consider using the TCP interface.

