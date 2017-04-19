#!/usr/bin/env python
""" This example demonstrates that you need to run
'pip install .' in the main directory, before you
can do 'import simple_cwl_xenon_service' in a Python program
"""

try:
    import simple_cwl_xenon_service
    print("Succesfully imported simple_cwl_xenon_service!")
except ImportError:
    print("Could not import simple_cwl_xenon_service! Maybe you forgot to run 'pip install'")
