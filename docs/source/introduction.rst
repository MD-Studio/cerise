Introduction
============

Cerise is a generic service for running workflows on compute resources, such as
clusters, supercomputers, and simply remote machines. It tries to offer a
consistent environment for workflows, so that a workflow sent to resource A will
work unchanged on resource B as well.

To achieve this, and to offer a bit of safety and perhaps security, Cerise does
not allow running arbitrary command line tools. Instead, it expects the user
to submit a workflow document that refers to predefined steps built into the
service. Both workflows and steps are defined using the
`Common Workflow Language`_ (CWL).

Defining these steps, and adding them to the service, is called specialising the
service. A specialisation of Cerise is always specific to a project and to a
compute resource. The project determines which steps are available and what
inputs and outputs they have. The compute resource determines how the steps are
implemented. Workflows are written using steps from a particular project, and
can then be sent to any specialisation to that project. Where the workflow runs
will differ depending on which specialisation is used, but the result should be
the same (assuming the calculation is deterministic!).

This site contains the documentation for Cerise.

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
specialise the service. For a detailed explanation, see
:ref:`specialising-cerise`.

To build the Docker image, use

    `docker build -t cerise .`

and then start a container using

    `docker run --name=cerise -p 29593:29593 cerise`

Note that the docker image gets its config.yml from conf/docker-config.yml in
the source tree.

However, this will run a plain, unspecialised Cerise, which is not very
useful, as it runs jobs locally inside the container, and it doesn't contain any
steps to execute. To use Cerise in Docker, you should make a new, specialised
Docker image based on the standard Cerise image, and start that instead.
Instructions for how to do so are also under :ref:`specialising-cerise`


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

.. _`Common Workflow Language`: http://www.commonwl.org/v1.0/UserGuide.html
