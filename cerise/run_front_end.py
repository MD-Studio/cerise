#!/usr/bin/env python3

import connexion

import sys
import os
import yaml

sys.path.append(os.path.join(os.path.dirname(__file__)))

from cerise.config import config
from cerise.front_end.encoder import JSONEncoder

app = connexion.App(__name__, specification_dir='front_end/swagger/')
app.app.json_encoder = JSONEncoder
app.add_api('swagger.yaml', base_path='/', arguments={'title': 'Cerise'})

application = app.app

if __name__ == '__main__':
    app.run(
        host=config['rest-service'].get('hostname', '127.0.0.1'),
        port=config['rest-service'].get('port', '5000')
        )
