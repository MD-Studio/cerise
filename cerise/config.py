import yaml

config = None
"""
The configuration dictionary, as parsed from the YAML
    config file.
"""

api_config = None
"""
The API configuration dictionary, as parsed from the YAML
    config file. Contains information about the compute
    resource to use.
"""

_config_file_path = 'conf/config.yml'
with open(_config_file_path) as config_file:
    config = yaml.safe_load(config_file)

_api_config_file_path = 'api/config.yml'
with open(_api_config_file_path) as api_config_file:
    api_config = yaml.safe_load(api_config_file)
