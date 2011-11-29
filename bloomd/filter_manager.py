"""
This module is used to manage the bloom filters in the
system. It maintains the set of filters available and
exports a clean API for use in the interfaces.
"""
import cPickle
import logging
import os
import os.path
import threading

from twisted.internet import task, reactor

from filters import Filter, ProxyFilter

# Prefix we add to bloomd directories
FILTER_PREFIX = "bloomd."

# Possible statuses
STATUS_READY = "ready"
STATUS_BUSY = "busy" # Concurrent reads/writes not permitted
STATUS_CLOSING = "closing" # Busy, and will not be ready
STATUS_FLUSHING = "flushing" # Busy, concurrent reads/writes permitted

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
        self.hot_filters = set([]) # Track the filters that are 'hot'
        self._schedule_flush = None
        self._schedule_cold = None

        # We use the filter status dictionary to map
        # the names of the filter onto a tuple that has
        # the status of the filter as the 0th index, and a Condition
        # object as the second. The status is one of "ready","busy",
        # "flushing", and "closing". This is used to protect
        # the filters, and provide finer grained locking of resources.
        # We use a special None key to control the entire dictionary.
        self.filter_status = {None:[STATUS_READY,threading.Condition()]}

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
            self.logger.info("Discovered filter: %s" % filter_name)

            filt = ProxyFilter(self, self.config, filter_name, full_path,
                              custom=load_custom_settings(full_path))
            self.filters[filter_name] = filt
            self.filter_status[filter_name] = [STATUS_READY,threading.Condition()]


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
            reactor.callInThread(self._flush_filter, name, filt)
        self.logger.debug("Ending scheduled flush.")

    def _flush_filter(self, name, filt):
        """
        Method that is called in a thread to flush a filter.
        This MUST NOT be called in the main thread, otherwise
        it could block the entire application.
        """
        # Check the filter status, wait for READY
        ready = self._wait_and_set_filter_status(name, STATUS_FLUSHING)
        if not ready: return

        # Do the flush now that the state is set
        filt.flush()

        # Restore back to ready
        self._notify_filter_status(name, STATUS_READY)

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
        # Check the filter status, wait for READY
        ready = self._wait_and_set_filter_status(name, STATUS_BUSY)
        if not ready: return

        # Replace with a proxy filter
        proxy_filt = ProxyFilter(self, filt.config, name, filt.path)
        proxy_filt.counters = filt.counters
        proxy_filt.counters.page_outs += 1
        self.filters[name] = proxy_filt

        # Close the existing filter
        filt.close()

        # Restore status
        self._notify_filter_status(name, STATUS_READY)

    def _wait_and_set_filter_status(self, name, set_status, wait_status=STATUS_READY, exit_status=STATUS_CLOSING):
        """
        Waits for the given status status, and sets it to the given one.
        If we see the exit_status (STATUS_CLOSING default), we bail and return False.
        If we see the wait_status (STATUS_READY default), then we update the status to set_status, and return True.
        """
        _, cond = self.filter_status[name]
        with cond:
            while True:
                status = self.filter_status[name][0]
                if status == exit_status: return False
                elif status != wait_status: cond.wait()
                else: break
            self.filter_status[name][0] = set_status
        return True

    def _notify_filter_status(self, name, status, all=False):
        "Sets the status of the given filter and notify waiters"
        _, cond = self.filter_status[name]
        with cond:
            self.filter_status[name][0] = status
            if all: cond.notify_all()
            else: cond.notify()

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

    def create_filter(self, name, custom=None):
        "Creates a new filter"
        path = os.path.join(self.config["data_dir"], FILTER_PREFIX+name)
        if not os.path.exists(path): os.mkdir(path)
        filt = Filter(self.config, name, path, custom=custom, discover=True)
        self.filters[name] = filt
        self.filter_status[name] = [STATUS_READY,threading.Condition()]
        self.hot_filters.add(name)
        return filt

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

