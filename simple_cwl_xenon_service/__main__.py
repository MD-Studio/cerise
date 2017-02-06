#!/usr/bin/env python3

import connexion

import sys
import os
sys.path.append(os.getcwd() + '/simple_cwl_xenon_service');

from swagger_server.encoder import JSONEncoder

if __name__ == '__main__':
    app = connexion.App(__name__, specification_dir='./swagger_server/swagger/')
    app.app.json_encoder = JSONEncoder
    app.add_api('swagger.yaml', arguments={'title': 'Simple CWL Xenon Service'})
    app.run(port=8080)
