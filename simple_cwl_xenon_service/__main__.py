#!/usr/bin/env python3

import connexion

import sys
import os
import yaml

sys.path.append(os.getcwd() + '/simple_cwl_xenon_service');

from swagger_server.encoder import JSONEncoder

if __name__ == '__main__':
    config_file_path = 'config.yml'
    with open(config_file_path) as config_file:
        config = yaml.load(config_file)

    app = connexion.App(__name__, specification_dir='./swagger_server/swagger/')
    app.app.json_encoder = JSONEncoder
    app.add_api('swagger.yaml', arguments={'title': 'Simple CWL Xenon Service'})
    # TODO: use config
    app.run(
        host=config['rest-service'].get('hostname', '127.0.0.1'),
        port=config['rest-service'].get('port', '5000')
        )
