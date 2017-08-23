Cerise
======
[![Develop build status](https://api.travis-ci.org/MD-Studio/cerise.svg?branch=develop)](https://travis-ci.org/MD-Studio/cerise) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/56de5791221a42e5964ba9d3a949c9c4)](https://www.codacy.com/app/LourensVeen/cerise) [![Coverage Badge](https://api.codacy.com/project/badge/Coverage/56de5791221a42e5964ba9d3a949c9c4)](https://www.codacy.com/app/LourensVeen/cerise?utm_source=github.com&utm_medium=referral&utm_content=MD-Studio/cerise&utm_campaign=Badge_Coverage) [![Documentation Status](https://readthedocs.org/projects/cerise/badge/?version=latest)](http://cerise.readthedocs.io/en/latest/?badge=latest)

This is a simple REST service that can run (some) CWL jobs on some remote
compute resource. It uses a REST API as its interface and PyXenon
to run jobs remotely.

The implementation is fairly complete, and the main things needed are some
real-world testing, bug fixing, and polish.


Documentation
-------------
Cerise is a generic service for running CWL workflows on compute resources (i.e.
clusters, supercomputers, and simply remote machines). It tries to offer a
consistent environment for workflows, so that a workflow sent to resource A will
work unchanged on resource B as well.

To achieve this, and to offer a bit of safety and perhaps security, Cerise does
not allow running arbitrary CWL command line tools. Instead, it expects the user
to submit a workflow document that refers to predefined steps built into the
service.

Defining these steps, and adding them to the service, is called specialising the
service. A specialisation of Cerise is always specific to a project (which
determines which steps are available and what inputs and outputs they have), and
to a compute resource (which determines how they are implemented). Thus, two
workflows sent to two different specialisations to the same project, but to
different compute resources, should give the same result (assuming the
calculation is deterministic!).

Documentation on how to specialise Cerise may be found in docs/specialising.rst.
Other documentation there covers configuring the Cerise service itself (i.e.
port numbers, logging configuration, etc.). There is also a requirements
document there, a detailed description of the design, and source code
documentation. You can also read the documentation online at
http://cerise.readthedocs.io/en/develop/


Installation
------------
Cerise can be run directly on a host, or in a Docker container. A local
installation is created as follows:

clone the repository
    `git clone git@github.com:MD-Studio/cerise.git`
change into the top-level directory
    `cd cerise`
install using
    `pip3 install .`

Steps and supporting files may then be placed in the api/ directory to
specialise the service. For a detailed explanation, see docs/specialising.rst.

To build the Docker image, use

    `docker build -t cerise .`

and then start a container using

    `docker run --name=cerise -p 29593-29594:29593-29594 cerise`

Note that the docker image gets its config.yml from conf/docker-config.yml in
the source tree.

However, this will run a plain, unspecialised Cerise, which is not very
useful, as it runs jobs locally inside the container, and it doesn't contain any
steps to execute. To use Cerise in Docker, you should make a new, specialised
Docker image based on the standard Cerise image, and start that instead.
Instructions for how to do so are also in docs/specialising.rst


Dependencies
------------
 * Python 3.5 or up

On the compute resource:
 * Python 2.7 and CWLTool (or another CWL runner), or
 * Python3 (using the built-in CWLTiny runner)

Example usage
-------------

In the examples/ directory, you will find some example Python scripts that
create jobs and execute them on the job running service.

Contribution guide
------------------
Cerise follows the Google Python style guide, with Sphinxdoc docstrings for module public functions. If you want to
contribute to the project please fork it, create a branch including your addition, and create a pull request.

The tests use relative imports and can be run directly after making
changes to the code. To run all tests use `pytest` in the main directory.
This will also run the integration tests, which take several minutes to complete
as a bunch of Docker containers is built, started, and stopped.

Before creating a pull request please ensure the following:
* You have written unit tests to test your additions
* All unit tests pass
* The examples still work and produce the same (or better) results
* The code is compatible with Python 3.5
* An entry about the change or addition is created in CHANGELOG.md
* You've added yourself as contributing author

Contributing authors so far:
* Lourens Veen
