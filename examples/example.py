#!/usr/bin/env python
""" This example demonstrates that you need to run
'pip install .' in the main directory, before you
can do 'import cerise' in a Python program
"""

try:
    import cerise
    print("Succesfully imported cerise!")
except ImportError:
    print("Could not import cerise! Maybe you forgot to run 'pip install'")
