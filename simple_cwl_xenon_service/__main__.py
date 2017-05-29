#!/usr/bin/env python3

import connexion

import sys
import os
import yaml

sys.path.append(os.getcwd() + '/simple_cwl_xenon_service')

from simple_cwl_xenon_service.config import config
from swagger_server.encoder import JSONEncoder

app = connexion.App(__name__, specification_dir='./swagger_server/swagger/')
app.app.json_encoder = JSONEncoder
app.add_api('swagger.yaml', base_path='/', arguments={'title': 'Simple CWL Xenon Service'})

application = app.app

if __name__ == '__main__':
    app.run(
        host=config('rest-service').get('hostname', '127.0.0.1'),
        port=config('rest-service').get('port', '5000')
        )
