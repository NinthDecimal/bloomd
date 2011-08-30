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
    - Create / List / Drop collections
    - Check / Set values in collections
    - Flush collections
    - Get configuration
    - Get collection stats
* Dead simple to start and administer

Install
-------

Download and install from source:
    
    python setup.py install

Usage
-----

Bloomd must be configured using a file which is in conf format.
Here is an example configuration file:

::

    # Settings for bloomd
    [bloomd]
    port = 8673
    data_dir = /mnt/bloomd
    log_dir = /var/log/bloomd.log
    log_level = INFO


Then run bloomd, pointing it to that file (assuming `/etc` right now)::

    bloomd -f /etc/bloomd.conf


Protocol
--------

By default, Bloomd will listen for TCP connections on port 8673.
It uses a simple ASCII protocol that is very similar to memcached.

A command has the following syntax:

    cmd [args][\r]\n

We start each line by specifying a command, providing optional arguments,
and ending the line in a newline (carriage return is optional).

There are a total of 8 commands:
* create - Create a new collection (a collection is a named bloom filter)
* list - List all collections
* drop - Drop a collection
* check - Check if a key is in a collection
* set - Set an item in a collection
* info - Gets info about a collection
* flush - Flushes all collections or just a specified one
* conf - Returns the default configuration, or the configuration of a single collection

For the ``create`` command, the format is:

    create collection_name [initial_size] [max_prob]

Where ``collection_name`` is the name of the collection,
and can contain the characters a-z, A-Z, 0-9, ., _.
If an initial size is provided (in bytes), the collection
will be created at that size, otherwise the configured value
will be used. If a maximum false positive probability is provided,
that will be used, otherwise the configured default is used.

As an example:

    create foobar 1048576 0.001

This will create a collection foobar that has a 1MB initial size,
and a 1/1000 probability of generating false positives. Valid responses
are either "Done", or "Exists".

The ``list`` command takes no arguments, and returns information
about all the collections. Here is an example response:

    START
    foobar 0.001 1048576 583450.315393 0
    END 

This indicates a single collection named foobar, with a probability
of 0.001 of false positives, a 1MB size, a current capacity of about
583K items, and 0 current items. The size and capacity automatically
scales as more items are added.

The ``drop`` command is like create, but only takes a collection name.
It can either return "Done" or "Does not exist".

Check and set look similar, they are either;

    [check|set] collection_name key

The command must specify a collection and a key to use.
They will either return "Yes", "No" or "Collection does not exist".

The ``info`` command takes a collection name, and returns
information about the collection. Here is an example output:

    START
    Probability 0.001
    Storage 1048576
    Capacity 583450.315393
    Size 0
    END

The command may also return "Does not exist" if the collection does
not exist.

The ``flush`` command may be called without any arguments, which
causes all collections to be flushed. If a collection name is provided
then that collection will be flushed. This will either return "Done" or
"Does not exist".

The final command ``conf`` is used to query the server configuration
or the collection configuration. Collections may have some custom parameters
when they are created, and store the configurations with them. They hold
some configuration which is not directly relevant to a collection.

    conf [collection_name]

An example output is:

    conf
    START
    scale_size 4
    default_probability 1e-06
    data_dir /tmp/bloomd
    probability_reduction 0.9
    initial_size 16777216
    initial_k 4
    flush_interval 60
    log_level DEBUG
    log_file /tmp/bloomd/bloomd.log
    port 8673
    END


