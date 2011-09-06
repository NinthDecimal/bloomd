"""
This module is responsible for reading and maintaining
the system configuration for BloomD.
"""
import sys
import os.path
from ConfigParser import ConfigParser, Error

def read_config(filename=None):
    if filename is None: filename='bloomd.cfg'
    cfp = ConfigParser()
    try:
        read = cfp.read([filename])
    except Error, e:
        raise EnvironmentError, ("Failed to parse the config file!", e)

    if filename != 'bloomd.cfg' and filename not in read:
        raise EnvironmentError, "Failed to read config file!"

    # Copy from defaults, update
    settings = dict(DEFAULTS)
    for key,val in DEFAULTS.iteritems():
        if not cfp.has_option("bloomd", key): continue
        provided = cfp.get("bloomd", key)
        expected_type = type(val)
        try:
            provided = expected_type(provided)
            settings[key] = provided
        except:
            print "Setting '%s' has invalid type!" % key
            sys.exit(1)

    # Perform custom validation
    for key,validator in VALIDATORS.iteritems():
        try:
            validator(settings[key])
        except Warning, e:
            print e
        except EnvironmentError, e:
            print e
            sys.exit(1)

    # Return the settings
    return settings

def valid_data_dir(dir):
    "Checks that the data dir is valid"
    if os.path.exists(dir) and not os.path.isdir(dir):
        raise EnvironmentError, "Providied data dir is not a directory!"
    try:
        if not os.path.exists(dir):
            os.mkdir(dir)
    except:
        raise EnvironmentError, "Cannot create data directory!"
    try:
        test_file = os.path.join(dir, "PERMTEST")
        fh = open(test_file, "a+")
        fh.close()
        os.remove(test_file)
    except:
        raise EnvironmentError, "Cannot write to data directory!"

def sane_log_file(log):
    "Checks that the log file is sane"
    try:
        fh = open(log, "a+")
        fh.close()
    except:
        raise EnvironmentError, "Cannot write to log file!"

def sane_scale_size(scale):
    "Checks the scale size is sane"
    if scale < 2:
        raise EnvironmentError, "Scale size must be at least 2!"
    elif scale > 4:
        raise Warning, "Scale size over 4 not recommended!"

def sane_probability(prob):
    "Checks the default probability is sane"
    if prob >= 1:
        raise EnvironmentError, "Probability cannot be more than 1!"
    elif prob > 0.01:
        raise Warning, "Probability set very high! Continuing..."
    elif prob <= 0:
        raise EnvironmentError, "Probability cannot be less than 0!"

def sane_probability_reduction(prob):
    "Checks the probability reduction is sane"
    if prob >= 1:
        raise EnvironmentError, "Probability reduction cannot be more than 1!"
    elif prob < 0.1:
        raise EnvironmentError, "Probability drop off is set too steep!"
    elif prob < 0.5:
        raise Warning, "Probability drop off is very steep!"

def valid_log_level(lvl):
    if lvl not in ("DEBUG","INFO","WARN","ERROR","CRITICAL"):
        raise EnvironmentError, "Invalid log level!"

def sane_initial_capacity(cap):
    "Checks the initial capacity is sane"
    if cap < 1000:
        raise EnvironmentError, "Initial capacity cannot be less than 1000!"
    elif cap > 1e9:
        raise Warning, "Initial capacity set very hig! Continuing..."

def sane_flush_interval(intv):
    "Checks that the flush interval is sane"
    if intv == 0:
        raise Warning, "Flushing is disabled! Data loss may occur."
    elif intv < 0:
        raise EnvironmentError, "Flushing interval must have a non-negative value!"
    elif intv >= 900:
        raise Warning, "Flushing set to be infrequent. This increases chances of data loss."

# Define our defaults here
DEFAULTS = {
    "port" : 8673,
    "data_dir" : "/tmp/bloomd",
    "log_file" : "/tmp/bloomd/bloomd.log",
    "log_level" : "DEBUG",
    "initial_capacity" : 1000*1000, # 1M
    "default_probability": 1E-4,
    "scale_size" : 4,
    "probability_reduction" : 0.9,
    "flush_interval" : 60,
}

VALIDATORS = {
    "data_dir": valid_data_dir,
    "log_level": valid_log_level,
    "log_file": sane_log_file,
    "initial_capacity": sane_initial_capacity,
    "default_probability": sane_probability,
    "scale_size": sane_scale_size,
    "probability_reduction": sane_probability_reduction,
    "flush_interval" : sane_flush_interval,
}

