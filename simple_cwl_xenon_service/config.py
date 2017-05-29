import yaml

_config_file_path = 'conf/config.yml'
with open(_config_file_path) as config_file:
    _config = yaml.safe_load(config_file)

def config(section):
    return _config[section]
