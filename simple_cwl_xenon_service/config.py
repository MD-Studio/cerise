import yaml

config = None
"""
The configuration dictionary, as parsed from the YAML
    config file.
"""

_config_file_path = 'conf/config.yml'
with open(_config_file_path) as config_file:
    config = yaml.safe_load(config_file)
