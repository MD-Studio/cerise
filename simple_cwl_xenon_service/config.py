import yaml

_config_file_path = 'conf/config.yml'
with open(_config_file_path) as config_file:
    _config = yaml.safe_load(config_file)

def config(section):
    """Return the configuration object.
    Args:
        section (Union[str, NoneType]): A specific section to get, or
            None.
    Returns:
        Dict: The configuration dictionary, or the given subdictionary
            if a section was specified.
    """
    if section is None:
        return _config
    else:
        return _config[section]
