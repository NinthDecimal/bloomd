"""
Supports incoming connections to our server. This module exports
the Twisted protocol we use to receive commands, as well as our
API implementation which glues our interface to the internal models.
"""
import logging
import re
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet import protocol

VALID_NAMES = re.compile("[a-zA-Z0-9._]+")

class APIHandler(object):
    "Implements the commands of our API. Method names map to the command name"
    # This must be set to the FilterManager we are using
    MANAGER = None

    @classmethod
    def create(cls, *args):
        if len(args) < 1: return "Client Error: Must provide collection name"
        name = args[0]

        # Sanity check the name
        if not VALID_NAMES.match(name):
            return "Client Error: Bad collection name"

        # Creates a new filter
        if name in cls.MANAGER:
            return "Exists"

        # Create a new filter
        custom = None
        if len(args) > 1:
            custom = {}
            if len(args) >= 2: custom["initial_size"] = int(args[1])
            if len(args) >= 3: custom["default_probability"] = float(args[2])
        cls.MANAGER.create_filter(name, custom)
        return "Done"

    @classmethod
    def list(cls, *args):
        ret = {}
        for key, filt in cls.MANAGER.filters.items():
            prob = filt.config["default_probability"]
            storage = filt.filter.total_bitmap_size()
            capacity = filt.filter.total_capacity()
            size = len(filt.filter)
            ret[key] = " ".join([str(s) for s in [prob, storage, capacity, size]])
        return ret

    @classmethod
    def drop(cls, *args):
        if len(args) < 1: return "Client Error: Must provide collection name"
        name = args[0]

        if name not in cls.MANAGER:
            return "Does not exist"
        else:
            del cls.MANAGER[name]
            return "Done"

    @classmethod
    def check(cls, *args):
        if len(args) < 2: return "Client Error: Must provide collection name and key"
        name = args[0]
        key = args[1]
        try:
            filt = cls.MANAGER[name]
            res = key in filt
            return "Yes" if res else "No"
        except KeyError:
            return "Collection does not exist"

    @classmethod
    def set(cls, *args):
        if len(args) < 2: return "Client Error: Must provide collection name and key"
        name = args[0]
        key = args[1]
        try:
            filt = cls.MANAGER[name]
            res = filt.add(key)
            return "Yes" if res else "No"
        except KeyError:
            return "Collection does not exist"

    @classmethod
    def info(cls, *args):
        if len(args) < 1: return "Client Error: Must provide collection name"
        name = args[0]
        if name not in cls.MANAGER: return "Does not exist"
        filt = cls.MANAGER[name]

        prob = "Probability "+str(filt.config["default_probability"])
        storage = "Storage "+str(filt.filter.total_bitmap_size())
        capacity = "Capacity "+str(filt.filter.total_capacity())
        size = "Size "+str(len(filt.filter))
        return [prob, storage, capacity, size]

    @classmethod
    def flush(cls, *args):
        # Check if we are flushing a specific collection
        if len(args) >= 1:
            name = args[0]
            if name not in cls.MANAGER: return "Does not exist"
            cls.MANAGER[name].flush()
            return "Done"

        # Flush all collections
        for name,filt in cls.MANAGER.filters.items():
            filt.flush()
        return "Done"

    @classmethod
    def conf(cls, *args):
        # Check for global settings, or a collections settings
        if len(args) == 0:
            return cls.MANAGER.config
        else:
            name = args[0]
            if name not in cls.MANAGER: return "Does not exist"
            ret = {"name":name}
            ret.update(cls.MANAGER[name].config)
            return ret

class ConnHandler(LineOnlyReceiver):
    "Simple Twisted Protocol handler to parse incoming commands"
    delimiter = "\n" # Use a plain newline, instead of \r\n
    MAX_LENGTH = 64*1024 # Change the line length to 64K
    LOGGER = logging.getLogger("bloomd.ConnHandler")

    @classmethod
    def getFactory(self):
        factory = protocol.ServerFactory()
        factory.protocol = ConnHandler
        return factory

    def lineReceived(self, line):
        # Split the line, at most 2 parts
        line_parts = line.strip().split(" ",2)
        cmd = line_parts[0]
        args = line_parts[1:]

        # Check if the APIHandler supports the command
        if hasattr(APIHandler, cmd):
            impl = getattr(APIHandler, cmd)
            try:
                res = impl(*args)

                # Handle the response
                if isinstance(res, (str, unicode)):
                    self.sendLine(res)

                elif isinstance(res, (list, tuple)):
                    self.sendLine("START")
                    for m in res:
                        self.sendLine(str(m))
                    self.sendLine("END")

                elif isinstance(res, dict):
                    self.sendLine("START")
                    for k,v in res.iteritems():
                        self.sendLine(k+" "+str(v))
                    self.sendLine("END")
            except:
                self.LOGGER.exception("Internal error running command '%s'" % cmd)
                self.sendLine("Internal Error")
        else:
            self.sendLine("Client Error: Command not supported")

