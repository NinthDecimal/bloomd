"""
This module is used to manage the bloom filters in the
system. It maintains the set of filters available and
exports a clean API for use in the interfaces.
"""
import time
import cPickle
import logging
import os
import os.path
import pyblooming as bloomlib
from twisted.internet import task

# Prefix we add to bloomd directories
FILTER_PREFIX = "bloomd."

def load_custom_settings(full_path):
    "Loads a custom configuration from a path, or None"
    config_path = os.path.join(full_path, "config")
    if not os.path.exists(config_path): return None
    raw = open(config_path).read()
    return cPickle.loads(raw)

class FilterManager(object):
    "Manages all the currently active filters in the system."
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("bloomd.FilterManager")
        self.filters = self._discover_filters()
        self.hot_filters = set([]) # Track the filters that are 'hot'
        self._schedule_flush = None
        self._schedule_cold = None

    def _discover_filters(self):
        "Called to discover existing filters"
        filters = {}
        content = os.listdir(self.config["data_dir"])
        for c in content:
            if FILTER_PREFIX not in c: continue
            full_path = os.path.join(self.config["data_dir"], c)
            if not os.path.isdir(full_path): continue

            filter_name = c.replace(FILTER_PREFIX, "")
            self.logger.info("Discovered filter: %s" % filter_name)

            filt = ProxyFilter(self, self.config, filter_name, full_path,
                              custom=load_custom_settings(full_path))
            filters[filter_name] = filt

        return filters

    def create_filter(self, name, custom=None):
        "Creates a new filter"
        path = os.path.join(self.config["data_dir"], FILTER_PREFIX+name)
        if not os.path.exists(path): os.mkdir(path)
        filt = Filter(self.config, name, path, custom=custom, discover=True)
        self.filters[name] = filt
        self.hot_filters.add(name)
        return filt

    def schedule(self):
        "Schedules the filter manager into the twisted even loop"
        if self.config["flush_interval"] == 0:
            self.logger.warn("Flushing is disabled! Data loss may occur.")
        else:
            self._schedule_flush = task.LoopingCall(self._flush)
            self._schedule_flush.start(self.config["flush_interval"],now=False)

        if self.config["cold_interval"] == 0:
            self.logger.warn("Cold filter unmapping is disabled!")
        else:
            self._schedule_cold = task.LoopingCall(self._unmap_cold)
            self._schedule_cold.start(self.config["cold_interval"],now=False)

    def _flush(self):
        "Called on a scheudle by twisted to flush the filters"
        self.logger.debug("Starting scheduled flush")
        start = time.time()
        for name,filt in self.filters.items():
            filt.flush()
        end = time.time()
        self.logger.debug("Ending scheduled flush. Total time: %f seconds" % (end-start))

    def _unmap_cold(self):
        "Called on a schedule by twisted to unmap cold filters"
        self.logger.debug("Starting unmap cold filters")
        start = time.time()

        all_filters = set(self.filters.keys())
        cold_filters = all_filters - self.hot_filters
        for name in cold_filters:
            # Ignore any ProxyFilters
            filt = self.filters[name]
            if isinstance(filt, ProxyFilter): continue

            # Close the actual filter
            self.logger.info("Unmapping filter '%s'" % name)
            filt.close()

            # Replace with a proxy filter
            proxy_filt = ProxyFilter(self, filt.config, name, filt.path)
            proxy_filt.counters = filt.counters
            proxy_filt.counters.page_outs += 1
            self.filters[name] = proxy_filt

        # Clear the hot filters
        self.hot_filters.clear()

        end = time.time()
        self.logger.debug("Ending scheduled cold unmap. Total time: %f seconds" % (end-start))

    def __getitem__(self, key):
        "Returns the filter"
        self.hot_filters.add(key)
        return self.filters[key]

    def __contains__(self, key):
        "Checks for the existence of a filter"
        return key in self.filters

    def __len__(self):
        "Returns the number of filters active"
        return len(self.filters)

    def __delitem__(self, key):
        "Deletes the filter"
        if key not in self.filters: return
        filt = self.filters[key]
        filt.close()
        filt.delete()
        del self.filters[key]

    def unmap(self, key):
        "Closes and unmaps a filter"
        if key not in self.filters: return
        filt = self.filters[key]
        filt.close()
        del self.filters[key]

    def close(self):
        "Prepares for shutdown, closes all filters"
        for name,filt in self.filters.items():
            filt.close()
            del self.filters[name]
        if self._schedule_flush:
            self._schedule_flush.stop()
            self._schedule_flush = None
        if self._schedule_cold:
            self._schedule_cold.stop()
            self._schedule_cold = None


class Counters(object):
    "Tracks opcounters"
    def __init__(self):
        self.set_hits = 0
        self.set_misses = 0
        self.check_hits = 0
        self.check_misses = 0
        self.page_outs = 0
        self.page_ins = 0

    @property
    def sets(self):
        return self.set_hits + self.set_misses

    @property
    def checks(self):
        return self.check_hits + self.check_misses

    def dict(self):
        return {"set_hits":self.set_hits,"set_misses":self.set_misses,
                "check_hits":self.check_hits,"check_misses":self.check_misses,
                "checks":self.checks,"sets":self.sets,"page_outs":self.page_outs,
                "page_ins":self.page_ins}

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

