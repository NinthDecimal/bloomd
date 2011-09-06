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

class FilterManager(object):
    "Manages all the currently active filters in the system."
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("bloomd.FilterManager")
        self.filters = self._discover_filters()

    def _discover_filters(self):
        "Called to discover existing filters"
        filters = {}
        content = os.listdir(self.config["data_dir"])
        for c in content:
            if FILTER_PREFIX not in c: continue
            full_path = os.path.join(self.config["data_dir"], c)
            if os.path.isdir(full_path):
                filter_name = c.replace(FILTER_PREFIX, "")
                self.logger.info("Discovered filter: %s" % filter_name)
                filt = Filter(self.config, filter_name, full_path, discover=True)
                filters[filter_name] = filt
        return filters

    def create_filter(self, name, custom=None):
        "Creates a new filter"
        path = os.path.join(self.config["data_dir"], FILTER_PREFIX+name)
        if not os.path.exists(path): os.mkdir(path)
        filt = Filter(self.config, name, path, custom=custom)
        self.filters[name] = filt

    def schedule(self):
        "Schedules the filter manager into the twisted even loop"
        if self.config["flush_interval"] == 0:
            self.logger.warn("Flushing is disabled! Data loss may occur.")
        schedule = task.LoopingCall(self._flush)
        schedule.start(self.config["flush_interval"])

    def _flush(self):
        "Called on a scheudle by twisted to flush the filters"
        self.logger.debug("Starting scheduled flush")
        start = time.time()
        for name,filt in self.filters.items():
            filt.flush()
        end = time.time()
        self.logger.debug("Ending scheduled flush. Total time: %f seconds" % (end-start))

    def __getitem__(self, key):
        "Returns the filter"
        return self.filters[key]

    def __contains__(self, key):
        "Checks for the existence of a filter"
        return key in self.filters

    def __delitem__(self, key):
        "Deletes the filter"
        if key not in self.filters: return
        filt = self.filters[key]
        filt.close()
        filt.delete()
        del self.filters[key]

    def close(self):
        "Prepares for shutdown, closes all filters"
        for name,filt in self.filters.items():
            filt.close()

class Filter(object):
    "Manages a single filter in the system."
    def __init__(self, config, name, full_path, custom=None, discover=False):
        self.logger = logging.getLogger("bloomd.Filter."+name)
        self.config = dict(config)
        if custom: self.config.update(custom)
        self.name = name
        self.path = full_path
        self.filenum = 0
        self.dirty = True
        if discover: self._discover()
        else: self._create_filter()

    def _discover(self):
        "Discovers the configuration for the filter"
        config_path = os.path.join(self.path, "config")
        if os.path.exists(config_path):
            self.logger.info("Found configuration file")
            raw = open(config_path).read()
            new_conf = cPickle.loads(raw)
            self.logger.info("Loaded config: %s" % new_conf)
            self.config.update(new_conf)

        # Discover the fragments
        fileparts = [f for f in os.listdir(self.path) if ".mmap" in f]
        fileparts.sort()
        self.filenum = len(fileparts)
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

    def _next_file(self, recount=True):
        "Returns the next filename to use for the scalable filter"
        if recount:
            fileparts = [f for f in os.listdir(self.path) if ".mmap" in f]
            self.filenum = len(fileparts)
        filename = os.path.join(self.path, "data.%03d.mmap" % self.filenum)
        self.logger.info("Adding new file '%s'" % filename)
        return filename

    def flush(self):
        "Invoked to force flushing the filter to disk"
        if not self.dirty: return
        # First, write out our settings
        start = time.time()
        config_path = os.path.join(self.path, "config")
        raw = cPickle.dumps(self.config)
        open(config_path, "w").write(raw)

        # Flush the filter
        self.filter.flush()
        end = time.time()
        self.logger.info("Flushing filter. Total time: %f seconds" % (end-start))
        self.dirty = False

    def close(self):
        "Flushes and cleans up"
        self.flush()
        self.filter.filenames = None
        self.filter.close()
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
        return key in self.filter

    def add(self, key):
        "Adds a key to the filter"
        self.dirty = True # Mark dirty
        return self.filter.add(key,True)

