"""
This module is used to manage the bloom filters in the
system. It maintains the set of filters available and
exports a clean API for use in the interfaces.
"""
import cPickle
import logging
import os
import os.path

from twisted.internet import task, reactor

from filters import Filter, ProxyFilter
from rwlock import ReadWriteLock

# Prefix we add to bloomd directories
FILTER_PREFIX = "bloomd."

def load_custom_settings(full_path, logger=None):
    "Loads a custom configuration from a path, or None"
    try:
        config_path = os.path.join(full_path, "config")
        tmp_config_path = os.path.join(full_path, "config")

        cp_exists = os.path.exists(config_path)
        tcp_exists = os.path.exists(tmp_config_path)
        if not (cp_exists or tcp_exists):
            return None

        filename = config_path if cp_exists else tmp_config_path
        raw = open(filename).read()
        return cPickle.loads(raw)
    except:
        if logger: logger.exception("Failed to load custom settings!")
        return {}

class FilterManager(object):
    "Manages all the currently active filters in the system."
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("bloomd.FilterManager")
        self.hot_filters = set([]) # Track the filters that are 'hot'
        self._schedule_flush = None
        self._schedule_cold = None

        # We use the filter status dictionary to map
        # the names of the filter onto a R/W Lock.
        # Write lock is required to create/drop/unmap filters.
        # Flushing just requires a read lock, since concurrent
        # reads and writes can take place
        self.filter_locks = {None:ReadWriteLock()}

        # Map of filter name to filter objects
        self.filters = {}
        self._discover_filters()

    def _discover_filters(self):
        "Called to discover existing filters"
        content = os.listdir(self.config["data_dir"])
        for c in content:
            if FILTER_PREFIX not in c: continue
            full_path = os.path.join(self.config["data_dir"], c)
            if not os.path.isdir(full_path): continue

            filter_name = c.replace(FILTER_PREFIX, "")
            try:
                filt = ProxyFilter(self, self.config, filter_name, full_path,
                                  custom=load_custom_settings(full_path, self.logger))
            except:
                self.logger.error("Failed to load filter: %s" % filter_name)
                continue

            self.filters[filter_name] = filt
            self.filter_locks[filter_name] = ReadWriteLock()


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
        self.logger.info("Starting scheduled flush")
        for name,filt in self.filters.items():
            if isinstance(filt, ProxyFilter): continue
            reactor.callInThread(self.flush_filter, name)
        self.logger.debug("Ending scheduled flush.")

    def flush_filter(self, name):
        """
        Method that is called in a thread to flush a filter.
        This MUST NOT be called in the main thread, otherwise
        it could block the entire application.
        """
        self.filter_locks[name].acquireRead()
        try:
            # Do the flush now that the state is set
            filt = self.filters[name]
            filt.flush()
        finally:
            self.filter_locks[name].release()

    def _unmap_cold(self):
        "Called on a schedule by twisted to unmap cold filters"
        self.logger.info("Starting unmap cold filters")

        all_filters = set(self.filters.keys())
        cold_filters = all_filters - self.hot_filters
        for name in cold_filters:
            # Ignore any ProxyFilters
            filt = self.filters[name]
            if isinstance(filt, ProxyFilter): continue

            # Close the actual filter
            self.logger.info("Unmapping filter '%s'" % name)
            reactor.callInThread(self._unmap_cold_filter, name, filt)

        # Clear the hot filters
        self.hot_filters.clear()

        self.logger.debug("Ending scheduled cold unmap.")

    def _unmap_cold_filter(self, name, filt):
        """
        Method that is called in a thread to unmap a filter.
        This MUST NOT be called in the main thread, otherwise
        it could block the entire application.
        """
        self.filter_locks[name].acquireWrite()
        try:
            # Replace with a proxy filter
            proxy_filt = ProxyFilter(self, filt.config, name, filt.path)
            proxy_filt.counters = filt.counters
            proxy_filt.counters.page_outs += 1
            self.filters[name] = proxy_filt

            # Close the existing filter
            filt.close()
        finally:
            self.filter_locks[name].release()

    def __contains__(self, key):
        "Checks for the existence of a filter"
        return key in self.filters

    def __len__(self):
        "Returns the number of filters"
        return len(self.filters)

    def check_keys(self, name, keys):
        """
        Checks if a filter with the given name contains a key.
        Returns a list of booleans if contained. May block execution,
        and is not safe to run in the main loop.

        Parameters:
            -`name`: The filter name
            -`keys`: The key values to check

        Raises KeyError if the filter does not exist.
        """
        self.filter_locks[name].acquireRead()
        try:
            filt = self.filters[name]
            self.hot_filters.add(name)
            return [key in filt for key in keys]
        finally:
            self.filter_locks[name].release()

    def set_keys(self, name, keys):
        """
        Sets mulitple keys in a filter with the given name.
        Return a list of booleans if added. May block execution,
        and is not safe to run in the main loop.

        Parameters:
            -`name`: The filter name
            -`keys`: The keys to add

        Raises KeyError if the filter does not exist.
        """
        self.filter_locks[name].acquireRead()
        try:
            filt = self.filters[name]
            self.hot_filters.add(name)
            return [filt.add(key) for key in keys]
        finally:
            self.filter_locks[name].release()

    def create_filter(self, name, custom=None):
        """
        Creates a new filter. This may block execution,
        and is not safe to run in the main event loop.

        Parameters:
            -`name` : The name of the filter
            -`custom` : Optional, custom parameters.
        """
        # Mark the entire dictionary as busy, blocks other creates
        self.filter_locks[None].acquireWrite()
        try:
            # Bail if the filter exists
            if name in self.filters: return self.filters[name]

            # Create the path
            path = os.path.join(self.config["data_dir"], FILTER_PREFIX+name)
            if not os.path.exists(path): os.mkdir(path)

            # Make a filter, not a proxy since it is probably hot
            filt = Filter(self.config, name, path, custom=custom, discover=True)
            self.filters[name] = filt
            self.filter_locks[name] = ReadWriteLock()
            self.hot_filters.add(name)
            return filt
        finally:
            self.filter_locks[None].release()

    def drop_filter(self, name):
        """
        Deletes the filter. This may block execution,
        and is not safe to run in the main event loop.

        Parameters:
            -`name` : The name of the filter to drop
        """
        # Use unmap, but also delete the filter
        self.unmap_filter(name, delete=True)

    def unmap_filter(self, name, delete=False):
        """
        Closes and unmaps a filter. This may block execution,
        and is not safe to run in the main event loop.

        Parameters:
            -`name` : The name of the filter to unmap
        """
        # Wait until ready, mark as closing
        if name not in self.filters: return
        self.filter_locks[name].acquireWrite()
        try:
            filt = self.filters[name]
            filt.close()
            if delete: filt.delete()
            del self.filters[name]
        finally:
            self.filter_locks[name].release()

    def close(self):
        "Prepares for shutdown, closes all filters"
        for name,filt in self.filters.items():
            try:
                filt.close()
            except:
                self.logger.exception("Error closing filter '%s'!" % name)
        self.filters.clear()
        if self._schedule_flush:
            self._schedule_flush.stop()
            self._schedule_flush = None
        if self._schedule_cold:
            self._schedule_cold.stop()
            self._schedule_cold = None

