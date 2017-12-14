Specialising Cerise
===================
(This document assumes some knowledge of the Common Workflow Language. See the
`CWL User Guide`_ for an introduction.)

Cerise works by letting users submit workflows to a REST API, and then executing
those workflows on some HPC compute resource. Users submit CWL Workflow
documents, which specify a number of steps to run. These steps are not submitted
by the user, but are part of the Cerise service. The base Cerise service does
not contain any steps, so those have to be added first, before Cerise can be
used in a project. This process of adding steps is known as specialisation.

A Cerise specialisation is always specific to a particular project, and
to a specific compute resource. The project determines which steps are available
and what their inputs and outputs are. Since HPC resources are all different in
terms of which software is installed and how things are best set up, the
implementation of the steps will be different from machine to machine, and so a
separate specialisation will have to be made for each one. (Hopefully
Singularity will help, once it sees more use, and in the cloud you have more
freedom of course, but we'll address the more difficult situation with
traditional HPC resources here.)

To specialise Cerise, you will have to design steps, implement them, and finally
add a bit of configuration information about how to connect to the compute
resource you are targeting.

General mechanism
-----------------
A Cerise install has an ``api/`` directory inside which files are placed to
specialise it. It uses the following directory structure:

``api/steps/<project>``
  Contains CWL Workflow and CWL CommandLineTool documents that can be used in
  user workflows. ``api/steps/cerise/`` is reserved for Cerise itself.

``api/files/<project>``
  Contains additional files needed to implement the steps. Typically, these will
  be shell scripts and/or binaries of necessary software. ``api/steps/cerise/``
  is reserved for Cerise itself, and contains ``cwltiny``, about which more
  below.

``api/config.yml``
  A YAML-format configuration file containing a description of how to connect to
  the compute resource.

Cerise executes workflows by staging them and their inputs to the compute
resource, and then running a CWL runner on the compute resource. The reference
implementation of CWL, ``cwltool``, is suitable for this, but is a complex Python
program with many dependencies, and it is not easy to get into an easily-staged
form. At least I haven't managed.

Cerise therefore comes with ``cwltiny``, a very limited CWL implementation that
consists of a single Python file with no dependencies. This will be used by
default. However, if you have or can get ``cwltool`` installed on your compute
resource, it will allow for more complex steps implementations, and for the user
to submit more complex workflows. In that case, you'll need to tell Cerise where
to find it in the config file.

A non-containerised Cerise install will take these files from the ``api/``
directory in its root. In the standard Docker container, this is in
``/home/cerise/api``. The recommended way of creating a specialised Cerise is to
create a new Docker image based on the ``mdstudio/cerise`` image, into which you
``COPY`` your own ``api`` directory. Be aware that the base image already
contains an ``api/files/cerise`` directory that it expects to continue to exist
(it contains ``cwltiny``), so using an overlay mount may not work, as that
hides the existing Cerise components.

Designing steps
---------------
The steps form the CWL API that your users will use when they specify workflows.
Which steps exist, and what goes in and out, depends entirely on your
application, so it's impossible to give general guidelines on what steps to
make. But there are a few technical issues to putting steps into Cerise.

Steps go into ``api/steps/<project>``, so that job running services that support
multiple projects are possible. So start by making that directory. Below it, you
can put CWL files in whatever directory structure you like. Users will refer to
the built-in steps via their path relative to ``api/steps``, so a user's
workflow will have ``run: <project>/mystep.cwl`` or something similar in it.
Cerise will ensure that the steps will be found by the runner.

Steps should be either CWL Workflows, or CWL CommandLineTools. I seem to end
up writing a small shell script and calling that from a CommandLineTool, but if
you can run a program directly then that will work as well of course. In a
CommandLineTool, you can use ``$CERISE_API_FILES`` at the start of the
``baseCommand``, and anywhere in the ``arguments`` to refer to the location of
supporting files in ``api/files``. Note that it is a simple string substitution,
so be sure to type it exactly like that. In particular, ``${CERISE_API_FILES}``
will not work.

Implementing steps
------------------
If the program you want to run is available by default on the compute resource,
then running it is as simple as providing an appropriate CWL CommandLineTool
definition for it. Often however, you'll need to do a bit or a lot of work to
get there. There are at least four ways of making the program you need available
on the compute resource you are specialising for:

1. Leave it to the user
2. Ask the system's administrator to install the software for you
3. Install it yourself where others can access it
4. Have Cerise stage the necessary files

Option 1 is the easiest of course, but you'll have to provide very precise
instructions to your users to ensure that they'll install the software exactly
where your step is expecting it. Also, the users may not be very happy about
having to jump through a bunch of hoops before being able to run their
calculations.

Option 2 is also pretty easy, but it may take a while for the system
administrator to get to your request, and they may refuse it for some reason. If
your request is granted, the software will typically be installed as a module,
so you'll need a ``module load`` command to make it available. The best solution
for this seems to be to stage a small shell script that does just that, and then
calls the program, passing on any arguments.

It would be nice if a CWL SoftwareRequirement could be used here to specify
which modules to load. Support for SoftwareRequirements in ``cwltool`` is still
in beta however, and ``cwltiny`` does not support it at all yet.

Option 3 can work if you have enough permissions, but has the downside that the
existence of the installation will probably depend on the existence of your
account. If your account is deleted, your users' services will be stuck without
the software they need.

Option 4 is how Cerise installs ``cwltiny``. When Cerise is started, it checks
for the existence of a ``.cerise`` directory (by default, see below) in the
user's home directory, and if it does not exist, it will create it and copy the
``api`` directory (and everything below it) there. This includes the steps, but
also ``api/files``, where any needed files can be put. The location where this
directory ends up can be referred to from your steps via ``$CERISE_API_FILES``
as described above.

Compute resource configuration
------------------------------
To complete the specialisation, a configuration file is needed with information
on how to connect to the compute resource. This configuration file must be
placed at `api/config.yml`. See :doc:`Cerise <configuration>` for what to put
into this file

.. _`CWL User Guide`: http://www.commonwl.org/v1.0/UserGuide.html
.. _Xenon: http://nlesc.github.io/Xenon/

