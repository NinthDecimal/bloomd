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
from twisted.internet import task

from filters import Filter, ProxyFilter

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

