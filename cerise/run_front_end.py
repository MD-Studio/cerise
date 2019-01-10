#!/usr/bin/env python3

import os
import sys

import connexion

sys.path.append(os.path.join(os.path.dirname(__file__)))
from cerise.config import make_config
from cerise.front_end.encoder import JSONEncoder


app = connexion.App(__name__, specification_dir='front_end/swagger/')
app.app.json_encoder = JSONEncoder
app.add_api('swagger.yaml', base_path='/', arguments={'title': 'Cerise'})

application = app.app

if __name__ == '__main__':
    config = make_config()
    app.run(host=config.get_service_host(), port=config.get_service_port())
