"""
Contains tests for the config module
"""
import tempfile
import os
import pytest
from bloomd import config

class TestConfig(object):
    def test_data_dir(self):
        "Tests our data diris sane"
        with pytest.raises(EnvironmentError):
            config.valid_data_dir("/home")
        with pytest.raises(EnvironmentError):
            config.valid_data_dir("/root")
        config.valid_data_dir("/tmp/testbloomd")

    def test_valid_log_level(self):
        "Tests the log levels"
        with pytest.raises(EnvironmentError):
            config.valid_log_level("foo")
        config.valid_log_level("INFO")

    def test_valid_log_level_lowercase(self):
        "Tests the log levels lowercased"
        with pytest.raises(EnvironmentError):
            config.valid_log_level("foo")
        config.valid_log_level("info")

    def test_log_file(self):
        "Tests our log file is sane"
        with pytest.raises(EnvironmentError):
            config.sane_log_file("/root.log")
        with pytest.raises(EnvironmentError):
            config.sane_log_file("/tmp")
        config.sane_log_file("/tmp/testlog.log")

    def test_inital_capacity(self):
        "Tests our initial capacity validation"
        with pytest.raises(EnvironmentError):
            config.sane_initial_capacity(0)
        with pytest.raises(EnvironmentError):
            config.sane_initial_capacity(-100)
        with pytest.raises(EnvironmentError):
            config.sane_initial_capacity(500)
        with pytest.raises(Warning):
            config.sane_initial_capacity(1e10)
        config.sane_initial_capacity(1e5)

    def test_probability(self):
        "Tests our probability validation"
        with pytest.raises(EnvironmentError):
            config.sane_probability(2)
        with pytest.raises(EnvironmentError):
            config.sane_probability(0)
        with pytest.raises(EnvironmentError):
            config.sane_probability(-1)
        config.sane_probability(0.001)

    def test_scale_size(self):
        "Tests our scale size validation"
        with pytest.raises(EnvironmentError):
            config.sane_scale_size(0)
        with pytest.raises(EnvironmentError):
            config.sane_scale_size(1)
        with pytest.raises(Warning):
            config.sane_scale_size(5)
        config.sane_scale_size(4)

    def test_probability_reduction(self):
        "Tests our probability reduction validation"
        with pytest.raises(EnvironmentError):
            config.sane_probability_reduction(1)
        with pytest.raises(EnvironmentError):
            config.sane_probability_reduction(0)
        with pytest.raises(EnvironmentError):
            config.sane_probability_reduction(0.01)
        with pytest.raises(Warning):
            config.sane_probability_reduction(0.4)
        config.sane_probability_reduction(0.8)

    def test_flush_interval(self):
        "Tests our flush interval validation"
        with pytest.raises(EnvironmentError):
            config.sane_flush_interval(-1)
        with pytest.raises(Warning):
            config.sane_flush_interval(0)
        with pytest.raises(Warning):
            config.sane_flush_interval(1800)
        config.sane_flush_interval(60)

    def test_cold_interval(self):
        "Tests our cold interval validation"
        config.sane_cold_interval(60)
        config.sane_cold_interval(3600)
        config.sane_cold_interval(86400)
        with pytest.raises(Warning):
            config.sane_cold_interval(0)
        with pytest.raises(EnvironmentError):
            config.sane_cold_interval(-1)

    def test_read_config_default(self):
        "Tests that read_config returns defaults with no args"
        assert config.read_config() == config.DEFAULTS

    def test_read_config_nofile(self):
        "Tests that read_config handles a missing file"
        with pytest.raises(EnvironmentError):
            config.read_config("/doesnotexist")

    def test_read_config_nofile(self):
        "Tests that read_config handles a missing file"
        with pytest.raises(EnvironmentError):
            config.read_config("/doesnotexist")

    def test_read_config_blank(self):
        "Tests that read_config handles a blank file"
        handle, name = tempfile.mkstemp()
        try:
            os.write(handle, "")
            assert config.read_config(name) == config.DEFAULTS
        finally:
            os.remove(name)

    def test_read_config_garbage(self):
        "Tests that read_config handles a garbage file"
        handle, name = tempfile.mkstemp()
        try:
            os.write(handle, "1234"*4096)
            with pytest.raises(EnvironmentError):
                config.read_config(name)
        finally:
            os.remove(name)

    def test_read_config_port(self):
        "Tests that read_config handles a garbage file"
        handle, name = tempfile.mkstemp()
        try:
            os.write(handle, "[bloomd]\nport=10000\n")
            settings = config.read_config(name)
            assert settings["port"] == 10000
        finally:
            os.remove(name)

    def test_read_config_port(self):
        "Tests that read_config handles a garbage file"
        conf = """[bloomd]
port = 10000
log_level = DEBUG
log_file = /tmp/testlog.log
initial_capacity = 100000
default_probability = 0.001
scale_size = 2
probability_reduction = 0.8
flush_interval = 120
"""
        handle, name = tempfile.mkstemp()
        try:
            os.write(handle, conf)
            settings = config.read_config(name)
            assert settings["port"] == 10000
            assert settings["log_level"] == "DEBUG"
            assert settings["log_file"] == "/tmp/testlog.log"
            assert settings["initial_capacity"] == 1e5
            assert settings["default_probability"] == 0.001
            assert settings["scale_size"] == 2
            assert settings["probability_reduction"] == 0.8
            assert settings["flush_interval"] == 120
        finally:
            os.remove(name)

