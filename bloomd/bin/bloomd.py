"""
Provides the entry point into bloomd with a main method.
"""
import sys
import logging
from optparse import OptionParser
from twisted.internet import reactor
from ..config import read_config
from ..filter_manager import FilterManager
from ..conn_handler import APIHandler, ConnHandler, MessageHandler

def setup_logging(config):
    "Configures our logging"
    # Setups basic file logging
    logging.basicConfig(filename=config["log_file"],
                        level=getattr(logging,config["log_level"]),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    # If we are on a tty, attach a stream handler
    if sys.stdout.isatty():
        stream = logging.StreamHandler()
        stream.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
        logging.getLogger('').addHandler(stream)

def dump_config(config):
    "Dumps our configuration"
    # Create the message
    msg = "Bloomd started! Dumping configuration:\n"

    # Add the configs
    config_keys = config.keys()
    config_keys.sort()
    for k in config_keys:
        msg += "\t"+k+" - "+str(config[k])+"\n"
    msg += "\n"

    logging.getLogger('bloomd').info(msg)

def start():
    "Starts running the event loop, cleanly exists"
    try:
        reactor.run()
    except:
        logging.getLogger('bloomd').exception("Caught an exception!")
    finally:
        shutdown()


def shutdown():
    "Performs a clean shutdown of the system"
    logging.getLogger('bloomd').info("Closing filters")
    APIHandler.MANAGER.close()


def main():
    "Main entry point, setup everything"
    # Parse command line args
    parser = OptionParser()
    parser.add_option("-f", "--file", action="store", dest="config_file",
                          default=None, help="Path to a configuration file")
    options, _ = parser.parse_args(sys.argv)

    # Load our configs
    try:
        config = read_config(options.config_file)
    except EnvironmentError:
        print "Failed to find and parse configuration!"
        sys.exit(1)
        return

    # Setup logging
    setup_logging(config)
    dump_config(config)

    # Setup the filter manager, schedule flushing
    APIHandler.MANAGER = FilterManager(config)
    APIHandler.MANAGER.schedule()

    # Setup listening for connections
    reactor.listenUDP(config["udp_port"],MessageHandler(),maxPacketSize=64*1024)
    logging.getLogger('bloomd').info("UDP Handler started on port %d" % config["udp_port"])
    reactor.listenTCP(config["port"],ConnHandler.getFactory())
    logging.getLogger('bloomd').info("TCP Handler started on port %d" % config["port"])

    # Start everything
    start()
    logging.getLogger('bloomd').info("Exiting")


if __name__ == "__main__":
    main()

