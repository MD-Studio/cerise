###########
Change Log
###########

All notable changes to this project will be documented in this file.
This project adheres to `Semantic Versioning <http://semver.org/>`_.

0.2.2
*****

Fixed
-----

* Improved network error handling

0.2.1
*****

Added
-----

* Faster API reinstall

Fixed
-----

* **Security** Overly-broad permissions on auto-created remote directories
* Improved network error handling

0.2.0
*****

Added
-----

* Better HPC support
  * Extra scheduler options (e.g. to select a GPU node)
  * Allow specifying the number of cores, per step
  * Allow specifying a time limit, per step and/or per job

* Improved specialisations
  * Allow multiple projects per specialisation
  * Versioning of specialisations
  * Automatic remote installation of updates
  * Rewritten documentation

Fixed
-----

* Usability
  * Improved job logs
  * Improved server log
  * Incremental remote job log updates
  * Log level configurable via environment variable

* Robustness
  * Automatically reconnect if the network connection drops
  * Better error-handling and retrying throughout
  * Better error messages
  * Database recovery in the event of a crash

* Technical improvements
  * Faster data transfer
  * Cleaner code, using Cerulean instead of Xenon 1
  * Better code documentation
  * Improved and cleaned up unit tests
  * Rebuilt integration test
  * Type annotations and checking using mypy

0.1.0
*****

Initial release
