import logging
import os
import os.path
import time
import cPickle

import pyblooming as bloomlib
from counters import Counters

class Filter(object):
    "Manages a single filter in the system."
    def __init__(self, config, name, full_path, custom=None, discover=False):
        self.logger = logging.getLogger("bloomd.Filter."+name)
        self.config = dict(config)
        if custom:
            self.config.update(custom)
            self.logger.info("Loaded custom configuration! Config: %s" % self.config)
        self.path = full_path
        if discover: self._discover()
        else: self._create_filter()
        self.dirty = True
        self.counters = Counters()
        self.counters.page_ins += 1

    def _discover(self):
        "Discovers the existing filter"
        # Discover the fragments
        fileparts = [f for f in os.listdir(self.path) if ".mmap" in f]
        fileparts.sort()
        self.logger.info("Found %d files: %s" % (len(fileparts), fileparts))

        # Get the bitmaps
        paths = [os.path.join(self.path, part) for part in fileparts]
        sizes = [os.path.getsize(part) for part in paths]
        bitmaps = [bloomlib.Bitmap(size,paths[i]) for i,size in enumerate(sizes)]
        filters = [bloomlib.BloomFilter(b,1) for b in bitmaps] # Use 1 k num, it will be re-read

        # Create the scalable filter
        self._create_filter(filters)

        # Print the stats
    def _create_filter(self, filters=None):
        "Creates a new instance of the scalable bloom filter"
        self.filter = bloomlib.ScalingBloomFilter(filters=filters,
                                                   filenames=self._next_file,
                                                   initial_capacity=self.config["initial_capacity"],
                                                   prob=self.config["default_probability"],
                                                   scale_size=self.config["scale_size"],
                                                   prob_reduction=self.config["probability_reduction"])
        self.logger.info("Adding Filter: Bitmap size: %d Capacity: %d Size: %d"
                         % (self.filter.total_bitmap_size(), self.filter.total_capacity(), len(self.filter)))

    def _next_file(self):
        "Returns the next filename to use for the scalable filter"
        fileparts = [f for f in os.listdir(self.path) if ".mmap" in f]
        filename = os.path.join(self.path, "data.%03d.mmap" % len(fileparts))
        self.logger.info("Adding new file '%s'" % filename)
        return filename

    def flush(self):
        "Invoked to force flushing the filter to disk"
        if not self.dirty: return
        # Save some information about the filters
        self.config["size"] = len(self)
        self.config["capacity"] = self.capacity()
        self.config["byte_size"] = self.byte_size()

        # First, write out our settings
        start = time.time()
        config_path = os.path.join(self.path, "config")
        raw = cPickle.dumps(self.config)
        open(config_path, "w").write(raw)

        # Flush the filter
        if self.filter is not None: self.filter.flush()
        end = time.time()
        self.logger.info("Flushing filter. Total time: %f seconds" % (end-start))
        self.dirty = False

    def close(self):
        "Flushes and cleans up"
        self.flush()
        if self.filter is not None:
            self.filter.filenames = None
            self.filter.close()
            self.filter = None
        self.logger.info("Closed filter")

    def delete(self):
        "Deletes the filter"
        # Remove all the files
        self.close()
        [os.remove(os.path.join(self.path,f)) for f in os.listdir(self.path) if (".mmap" in f or f == "config")]

        # Remove the dir
        os.rmdir(self.path)
        self.logger.info("Deleted filter")

    def __contains__(self, key):
        "Checks if a key is contained"
        res = key in self.filter
        if res: self.counters.check_hits += 1
        else: self.counters.check_misses += 1
        return res

    def add(self, key):
        "Adds a key to the filter"
        self.dirty = True # Mark dirty
        res = self.filter.add(key,True)
        if res: self.counters.set_hits += 1
        else: self.counters.set_misses += 1
        return res

    def __len__(self):
        "Returns the number of items in the filter"
        return len(self.filter)

    def capacity(self):
        "Returns the current capacity of the filter"
        return self.filter.total_capacity()

    def byte_size(self):
        "Returns the current byte size of the filter"
        return self.filter.total_bitmap_size()


class ProxyFilter(object):
    "Manages a single filter in the system."
    def __init__(self, manager, config, name, full_path, custom=None):
        self.manager = manager
        self.name = name
        self.logger = logging.getLogger("bloomd.ProxyFilter."+name)
        self.config = dict(config)
        self.config.setdefault("size",0)
        self.config.setdefault("capacity",self.config["initial_capacity"])
        self.config.setdefault("byte_size",0)
        if custom:
            self.config.update(custom)
            self.logger.info("Loaded custom configuration! Config: %s" % self.config)
        self.path = full_path
        self.counters = Counters()

    def __getattribute__(self, attr):
        "High-jack some methods to simplify things"
        if attr in ("flush","close"):
            return lambda *args,**kwargs : None
        elif attr in ("capacity","byte_size"):
            return lambda : self.config[attr]
        else:
            return object.__getattribute__(self, attr)

    def _fault_attr(self, attr, *args, **kwargs):
        self.logger.info("Faulting in the real filter!")
        filter = Filter(self.config, self.name, self.path, discover=True)
        filter.counters = self.counters # Copy our counters over
        filter.counters.page_ins += 1 # Increment the page in count
        self.manager.filters[self.name] = filter # Replace the proxy with the real deal
        return getattr(filter, attr)(*args, **kwargs)

    def __len__(self):
        return self.config["size"]

    def __contains__(self, *args, **kwargs):
        return self._fault_attr("__contains__", *args, **kwargs)

    def add(self, *args, **kwargs):
        return self._fault_attr("add", *args, **kwargs)

    def delete(self):
        "Deletes the filter"
        [os.remove(os.path.join(self.path,f)) for f in os.listdir(self.path) if (".mmap" in f or f == "config")]
        os.rmdir(self.path)
        self.logger.info("Deleted filter")

