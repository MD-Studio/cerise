#!/usr/bin/env python
""" This example demonstrates that you need to run
'pip install .' in the main directory, before you
can do 'import simple-cwl-xenon-service' in a Python program

The from __future__ import below is to make this
code compatible with both Python 2 and 3
"""
from __future__ import print_function

try:
    import simple-cwl-xenon-service
    print("Succesfully imported simple-cwl-xenon-service!")
except ImportError:
    print("Could not import simple-cwl-xenon-service! Maybe you forgot to run 'pip install'")
