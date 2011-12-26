"""
Supports incoming connections to our server. This module exports
the Twisted protocol we use to receive commands, as well as our
API implementation which glues our interface to the internal models.
"""
import logging
import re
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet.protocol import DatagramProtocol, ServerFactory
import config
import socket

VALID_NAMES = re.compile("[a-zA-Z0-9._]+")

class APIHandler(object):
    "Implements the commands of our API. Method names map to the command name"
    # This must be set to the FilterManager we are using
    MANAGER = None

    @classmethod
    def create(cls, *args):
        if len(args) < 1: return "Client Error: Must provide filter name"
        name = args[0]

        # Sanity check the name
        if not VALID_NAMES.match(name):
            return "Client Error: Bad filter name"

        # Creates a new filter
        if name in cls.MANAGER:
            return "Exists"

        # Create a new filter
        custom = None
        if len(args) > 1:
            args = args[1].split(" ")
            custom = {}
            if len(args) >= 1:
                try:
                    capacity = custom["initial_capacity"] = int(args[0])
                    config.sane_initial_capacity(capacity)
                except (ValueError, EnvironmentError):
                    return "Client Error: Bad initial capacity!"
                except Warning:
                    pass
            if len(args) >= 2:
                try:
                    prob = custom["default_probability"] = float(args[1])
                    config.sane_probability(prob)
                except (ValueError, EnvironmentError):
                    return "Client Error: Bad false positive probability!"
                except Warning:
                    pass

        cls.MANAGER.create_filter(name, custom)
        return "Done"

    @classmethod
    def list(cls, *args):
        ret = {}
        for key, filt in cls.MANAGER.filters.items():
            try:
                prob = filt.config["default_probability"]
                storage = filt.byte_size()
                capacity = filt.capacity()
                size = len(filt)
                ret[key] = " ".join([str(s) for s in [prob, storage, capacity, size]])
            except:
                # Ignore things if we get an error, that filter
                # might be in a state of flux
                pass
        return ret

    @classmethod
    def drop(cls, *args):
        if len(args) < 1: return "Client Error: Must provide filter name"
        name = args[0]
        try:
            if name not in cls.MANAGER: raise KeyError
            cls.MANAGER.drop_filter(name)
            return "Done"
        except KeyError:
            return "Filter does not exist"

    @classmethod
    def close(cls, *args):
        if len(args) < 1: return "Client Error: Must provide filter name"
        name = args[0]
        try:
            if name not in cls.MANAGER: raise KeyError
            cls.MANAGER.unmap_filter(name)
            return "Done"
        except KeyError:
            return "Filter does not exist"

    @classmethod
    def c(cls, *args):
        return cls.check(*args)

    @classmethod
    def check(cls, *args):
        if len(args) < 2: return "Client Error: Must provide filter name and key"
        name = args[0]
        key = args[1]
        try:
            res = cls.MANAGER.check_keys(name, [key])
            return "Yes" if res[0] else "No"
        except KeyError:
            return "Filter does not exist"

    @classmethod
    def m(cls, *args):
        return cls.bulk(*args)

    @classmethod
    def multi(cls, *args):
        if len(args) < 2: return "Client Error: Must provide filter name and at least one key"
        name = args[0]
        keys = args[1].strip().split(" ")
        try:
            results = cls.MANAGER.check_keys(name, keys)
            results = ["Yes" if r else "No" for r in results]
            return " ".join(results)
        except KeyError:
            return "Filter does not exist"

    @classmethod
    def s(cls, *args):
        return cls.set(*args)

    @classmethod
    def set(cls, *args):
        if len(args) < 2: return "Client Error: Must provide filter name and key"
        name = args[0]
        key = args[1]
        try:
            res = cls.MANAGER.set_keys(name, [key])
            return "Yes" if res[0] else "No"
        except KeyError:
            return "Filter does not exist"

    @classmethod
    def b(cls, *args):
        return cls.bulk(*args)

    @classmethod
    def bulk(cls, *args):
        if len(args) < 2: return "Client Error: Must provide filter name and at least one key"
        name = args[0]
        keys = args[1].strip().split(" ")
        try:
            results = cls.MANAGER.set_keys(name, keys)
            results = ["Yes" if r else "No" for r in results]
            return " ".join(results)
        except KeyError:
            return "Filter does not exist"

    @classmethod
    def info(cls, *args):
        if len(args) < 1: return "Client Error: Must provide filter name"
        name = args[0]
        try:
            # Use the filters directly to avoid adding to the hot list
            filt = cls.MANAGER.filters[name]
        except KeyError:
            return "Filter does not exist"
        res = {}
        res["probability"] = str(filt.config["default_probability"])
        res["storage"] = str(filt.byte_size())
        res["capacity"] = str(filt.capacity())
        res["size"] =  str(len(filt))
        res.update(filt.counters.dict())

        return res

    @classmethod
    def flush(cls, *args):
        # Check if we are flushing a specific filter
        if len(args) >= 1:
            name = args[0]
            try:
                cls.MANAGER.flush_filter(name)
                return "Done"
            except KeyError:
                return "Filter does not exist"

        # Flush all filters
        for name in cls.MANAGER.filters.keys():
            try:
                cls.MANAGER.flush_filter(name)
            except KeyError:
                pass # May have been deleted/unmapped
        return "Done"

    @classmethod
    def conf(cls, *args):
        # Check for global settings, or a filters settings
        if len(args) == 0:
            return cls.MANAGER.config
        else:
            name = args[0]
            try:
                ret = {"name":name}
                ret.update(cls.MANAGER.filters[name].config)
                return ret
            except KeyError:
                return "Filter does not exist"

class ConnHandler(LineOnlyReceiver):
    "Simple Twisted Protocol handler to parse incoming commands"
    delimiter = "\n" # Use a plain newline, instead of \r\n
    MAX_LENGTH = 64*1024 # Change the line length to 64K
    LOGGER = logging.getLogger("bloomd.ConnHandler")

    @classmethod
    def getFactory(self):
        factory = ServerFactory()
        factory.protocol = ConnHandler
        return factory

    def lineReceived(self, line):
        # Split the line, at most 2 parts
        line = line.rstrip("\r\n")
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
                    keys = res.keys()
                    keys.sort()
                    for k in keys:
                        self.sendLine(k+" "+str(res[k]))
                    self.sendLine("END")
            except:
                self.LOGGER.exception("Internal error running command '%s'" % cmd)
                self.sendLine("Internal Error")
        else:
            self.sendLine("Client Error: Command not supported")

class MessageHandler(DatagramProtocol):
    """
    Simple Twisted Protocol handler to parse incoming messages.
    This allows clients to send UDP packets with commands instead
    of creating a TCP connection to setup the handshake. The difference
    is that we never respond to commands, and acts more like a fire and
    forget method. This is useful for efficient high-volume set commands.
    """
    LOGGER = logging.getLogger("bloomd.MessageHandler")

    def startProtocol(self):
        "Hook into the protocol start to set the buffer size"
        for buff_size in (4*1024**2,2*1024**2,1024**2,512*1024):
            try:
                self.transport.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
                return
            except:
                pass

    def datagramReceived(self, datagram, addr):
        # Handle each line in the datagram
        lines = datagram.split("\n")
        for line in lines:
            # Split the line, at most 2 parts
            line = line.rstrip("\r\n")
            line_parts = line.strip().split(" ",2)
            cmd = line_parts[0]
            args = line_parts[1:]

            # Check if the APIHandler supports the command
            if hasattr(APIHandler, cmd):
                impl = getattr(APIHandler, cmd)
                try:
                    impl(*args)
                except:
                    self.LOGGER.exception("Internal error running command '%s'" % cmd)

