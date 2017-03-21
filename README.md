Simple CWL Xenon Service
========================
[![Build Status](https://api.travis-ci.org/LourensVeen/simple-cwl-xenon-service.svg?branch=master)](https://travis-ci.org/LourensVeen/simple-cwl-xenon-service)

This is a simple REST service that can run (some) CWL jobs on some remote
compute resource. It uses the GA4GH REST API as its interface and PyXenon
to run jobs remotely.

Note that this is very much a prototype at the moment, and that we are
planning to create a more robust implementation in the future. This
project serves as a pathfinder, with mistakes to be made and learnt
from. It's also my first non-trivial Python program, so it will be a bit
messy, but that will get better over time as I learn to omit semicolons
and such.

Documentation
-------------
Documentation may be found in the doc/ directory. Currently there are
the requirements for this project there, and a probably somewhat outdated
class diagram.

Installation
------------
clone the repository
    `git clone git@github.com:LourensVeen/simple-cwl-xenon-service.git`
change into the top-level directory
    `cd simple-cwl-xenon-service`
install using
    `pip3 install .`

Dependencies
------------
 * Python 3.5 or up

Example usage
-------------
To start the service, simply run

    `python3 -m simple_cwl_xenon_service`

and open your browser to here:

```
http://localhost:29593/ui/
```

The Swagger definition of the interface can be found at

```
http://localhost:29593/swagger.json
```

Docker
------

To build a Docker image, use
    `cd simple-cwl-xenon-service`
    `docker build -t simple-cwl-xenon-service .`
then run it using
    `docker run --name=simple-cwl-xenon-service -p 29593 simple-cwl-xenon-service`
and point your browser to

```
http://localhost:29593/ui/
```

Note that the Dockerfile uses config-docker.yml for configuration.

Contribution guide
------------------
The simple-cwl-xenon-service project follows the Google Python style guide, with Sphinxdoc docstrings for module public functions. If you want to
contribute to the project please fork it, create a branch including your addition, and create a pull request.

The tests use relative imports and can be run directly after making
changes to the code. To run all tests use `pytest` in the main directory.
To run the examples after code changes, you need to run `pip install --upgrade .`
Documentation is generated by typing `make html` in the doc directory,
the contents of doc/build/html/ should then be copied to the right directory of your gh-pages branch.

Before creating a pull request please ensure the following:
* You have written unit tests to test your additions
* All unit tests pass
* The examples still work and produce the same (or better) results
* The code is compatible with Python 3.5
* An entry about the change or addition is created in CHANGELOG.md
* Add yourself as contributing author

Contributing authors so far:
* Lourens Veen
