An empty Python project
=======================
[![Build Status](https://api.travis-ci.org/LourensVeen/simple-cwl-xenon-service.svg?branch=master)](https://travis-ci.org/LourensVeen/simple-cwl-xenon-service)

The goal of this repository is to serve as an example of how to structure
a newly started Python project for the Netherlands eScience Center, being
compliant for as much as possible with the [eStep checklist](https://github.com/NLeSC/estep-checklist).

The idea is that this is a collaborative effort so feel free to directly
contribute or suggest any additions and/or changes that may improve this
empty Python project repository.

Documentation
-------------
You could include a link to the full documentation of your project here.

Installation
------------
clone the repository  
    `git clone git@github.com:LourensVeen/simple-cwl-xenon-service.git`  
change into the top-level directory  
    `cd simple-cwl-xenon-service`  
install using  
    `pip install .`

Dependencies
------------
 * Python 2.7 or Python 3.5

Example usage
-------------
To use this repository as the starting point for your Python project, just
download the repository and rename all occurences of `simple-cwl-xenon-service` with the
name of your new project!

Contribution guide
------------------
The simple-cwl-xenon-service Project follows the Google Python style guide, with Sphinxdoc docstrings for module public functions. If you want to
contribute to the project please fork it, create a branch including your addition, and create a pull request.

The tests use relative imports and can be run directly after making
changes to the code. To run all tests use `nosetests` in the main directory.
To run the examples after code changes, you need to run `pip install --upgrade .`
Documentation is generated by typing `make html` in the doc directory,
the contents of doc/build/html/ should then be copied to the right directory of your gh-pages branch.

Before creating a pull request please ensure the following:
* You have written unit tests to test your additions
* All unit tests pass
* The examples still work and produce the same (or better) results
* The code is compatible with both Python 2.7 and Python 3.5
* An entry about the change or addition is created in CHANGELOG.md
* Add yourself as contributing author

Contributing authors so far:
* Lourens Veen
