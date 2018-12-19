.. _specialising-cerise:

Specialising Cerise
===================
(This document assumes some knowledge of the Common Workflow Language. See the
`CWL User Guide`_ for an introduction.)

Cerise works by letting users submit workflows to a REST API, and then executing
those workflows on some HPC compute resource. Users submit CWL Workflow
documents, which specify a number of steps to run. Step definitions are not
submitted by the user, but are part of the Cerise service. The base Cerise
service does not contain any steps, so those have to be added first, before
Cerise can be used in a project. This process of adding steps is known as
specialisation.

A Cerise specialisation is always specific to a particular project, and
to a specific compute resource. The project determines which steps are available
and what their inputs and outputs are. Since HPC resources are all different in
terms of which software is installed and how things are best set up, the
implementation of the steps will be different from machine to machine, and so a
separate specialisation will have to be made for each one. (Hopefully
Singularity will help, once it sees more use, and in the cloud you have more
freedom of course, but we'll address the more difficult situation with
traditional HPC resources here.)

To specialise Cerise, you will have to make a configuration file that describes
how to connect to a compute resource, design steps, and implement them. The
easiest way to use the specialisation is to wrap it up into a Docker container
together with Cerise, so you'll have a ready-to-run service.

These steps are described in more detail below, using an example that you can
find in
`docs/examples/specialisation
<https://github.com/MD-Studio/cerise/tree/master/docs/examples/>`_ in the
source distribution. Note that you'll need Docker installed, if you don't have
it yet, see `the Docker Community Edition documentation
<https://docs.docker.com/install/>`_ on how to install it.

The API configuration file
--------------------------

The main directory of a specialisation is called ``api/``, because a
specialisation specifies what the CWL API looks like and how it's implemented.
Inside of this directory is a configuration file named ``api/config.yml``. The
example's configuration file looks like this:

.. literalinclude:: ../examples/specialisation/api/config.yml
  :caption: ``docs/examples/specialisation/api/config.yml``
  :language: yaml


An API configuration file is a YAML file describing the compute resource to run
on. It has a single top-level key `compute-resource`, with below it keys for
`files`, `jobs` and `refresh`. There are many other options (see
:ref:`compute-resource-configuration`), what you see here is the simplest possible
configuration.

Cerise needs two things from the compute resource it runs on: a place to store
files (including workflows, steps, input and output, as well as any files that
are installed by this specialisation), and a way of submitting jobs. The files
are stored in a directory on the remote system, given by `path` under `files`.
In this case, we'll put them into `/home/cerise/.cerise`, and we recommend using
a `.cerise` directory in the user's home directory. (This is fine even if your
user is also using a different specialisation that uses the same `.cerise`
directory.)

The other thing Cerise needs is a way to get files into and out of that
directory, a `protocol`. In this case, we're using the file system inside of the
Docker container, so `local` is appropriate.

Next, we need to specify how to run jobs on the machine. This requires a
`protocol` key again, this time specifying the protocol to use to connect to the
machine when submitting jobs. We again use `local` here. Since there's no
scheduler (like Slurm or Torque, as you'd find on a typical compute cluster)
inside of the Docker container, we don't specify one. If no scheduler is
specified, Cerise will start jobs directly on the machine, in this case inside
of the Docker container.

Finally, we specify a refresh interval in seconds, which tells Cerise how often
to check on the progress of running jobs. When you're submitting long-running
jobs to a cluster or HPC machine, something like half a minute or a minute is
appropriate.  That will keep Cerise from hammering the head node with requests,
and for a long-running job it's okay if it takes a bit longer for Cerise to
realise that the job is done. For our testing purposes it's nice to have a
result quickly, and since we're running locally we're not bothering anyone by
updating more often, so in the example the refresh interval is set to 0.1
second.

The Dockerfile
--------------

The easiest way to run Cerise is inside of a Docker container. Note that this
does not mean that you need Docker support on the compute resource. Cerise runs
locally inside the container, and connects to the resource to run workflows
there. Docker containers are built using a Dockerfile, which describes the steps
needed to install the needed software and set up the system.

Cerise comes with its own Docker image, which is available on Dockerhub, and all
that's really needed to specialise it is to copy your API into it. So all our
Dockerfile needs to do is to start with the Cerise image, and copy the ``api/``
dir inside:

.. code-block:: dockerfile

    FROM cerise

    COPY api /home/cerise/api


To build the test image, make sure you are inside the `specialisation`
directory, then type:

.. code-block:: bash

    docker build -t cerise-example .

This will build a new Docker image with your API inside it and name it
`cerise-example`.

It's possible to test the image by starting it manually and making HTTP requests
to it using `curl`, but it's much easier to use Python and Cerise Client. We'll
make a virtualenv and install Cerise Client in it first:

.. code-block:: bash

    virtualenv -p python3 ./env
    . ./env/bin/activate
    pip install cerise_client


Next, you can start Python 3 interactively, or make a simple Python script
that starts a Cerise service and runs a job:

.. literalinclude:: ../examples/specialisation/test_script.py
  :caption: ``docs/examples/specialisation/test_script.py``


In this example, we run a test workflow that looks like this:

.. literalinclude:: ../examples/specialisation/test_workflow.cwl
  :caption: ``docs/examples/specialisation/test_workflow.cwl``
  :language: yaml


This workflow runs a single step, the ``cerise/test/hostname.cwl`` step, which
is built in to the base Cerise image. This step takes no inputs, and produces a
single output named `output`, a file containing the output of the `hostname`
command. The test script prints its contents, which will be something like
`aad7da47e423`, because Docker containers by default have a random host name
that looks like that, and we're running inside of the container.

Adding steps
------------

So far, our specialisation only has a configuration file. For an actual project,
you will want to add one or more steps for running the software you need. Here,
we'll add two steps, a very simple one that runs a program that is already
present on the target machine, and a more complex one that requires installing
some software remotely.

The Cerise API is organised by project. A project is simply a collection of CWL
steps, plus the additional files needed to make them work, wrapped up in a
directory. If multiple people work on specialisations for the same project, then
they'll have to coordinate their efforts in order to avoid messing up each
other's work, but developers on different projects can do their own thing
without getting into each other's way. (Even if they use the same remote working
directory. You can run two specialisations for different projects and the same
machine simultaneously on the same account.)

While it's possible to have multiple projects in a single specialisation, for
example in a Cerise-as-infrastructure case where you have a single Cerise
instance with a cluster behind it and multiple users that want to do different
things, in most cases you'll have only one project per specialisation. So that
is what we'll assume here.

A simple step
'''''''''''''

Let's start with a simple step that returns the hostname of the machine it is
running on, like before, but this time as part of our own project, named
``example``. We'll add a new CWL file to the specialisation, in
``api/example/steps/example/hostname.cwl``:

.. literalinclude:: ../examples/specialisation/api/example/steps/example/hostname.cwl
  :caption: ``docs/examples/specialisation/api/example/steps/example/hostname.cwl``


Since the `hostname` command is available on any Linux machine, we don't need to
do anything else for this step to work. To call it, just modify
`test_workflow.cwl` to use ``example/hostname.cwl`` instead of
``cerise/test/hostname.cwl``. Note that the name to use in the workflow is the
path relative to your `steps/` directory.

This means that the submitted workflow does not contain the full path to the
step. This is good, because the full path changes per machine and per user (who
each have a home directory with a different name), and besides it being annoying
for the user if they have to look up the full path every time they write a
workflow, it would mean that workflows become machine- and user-specific. This
is something that Cerise is designed to avoid.

Instead, Cerise expects the user to give a relative path starting with the name
of the project. When it copies the user's workflow to the compute resource in
preparation for running it, Cerise extracts the project name from the first part
in the path, and then prepends the path with the absolute path to the remote
`steps/` directory for that project. As a result, the CWL runner that executes
the workflow can find the steps. It only works however if your steps are in
``<project>/steps/<project>/``, so be sure to follow that pattern!

How Cerise installs the API
'''''''''''''''''''''''''''

In order to find out how to set up more complex steps that
require software installation, it's good to know a bit about how Cerise installs
your API on the compute resource.

When Cerise is started, it logs in to the configured compute resource, and
checks to see if the API has been installed there already. If not, it will
create the configured base file path (from your `config.yml`), create an `api/`
directory inside it, and copy your project directory into that.

While copying the steps, Cerise will replace any occurrence of
`$CERISE_PROJECT_FILES` in the `baseCommand` or `arguments` with the location of
your `files/` directory. This allows you to run programs in your files
directory, or pass locations of files in your `files/` directory to programs
that you run.

After the steps and the files are copied, Cerise will check whether a file named
`install.sh` exists in your `files/` directory. If it does, Cerise will run it
remotely. This script (or whatever you put there) will run with an environment
that has `$CERISE_PROJECT_FILES` set. It's probably a bad idea to modify
anything outside of the `files/` directory from this script, so don't do that
(if you have a good reason to do so, we'd love to hear from you, please make an
issue on GitHub!).

Debugging a specialisation
''''''''''''''''''''''''''

The new steps you're adding will likely not work immediately. Just like with any
kind of programming or configuring, it usually takes a few tries to get it
right. If there is something wrong with your install script or your steps, then
it may happen that Cerise fails to start. In this case, no jobs can be
submitted, and you need the server log to figure out what's going on. You'll
find a few commented-out lines in `test_script.py` that print the server log for
you. You can also get to the file by hand, provided that the container still
exists, using the command

.. code-block:: bash

    docker cp cerise-example-test:/var/log/cerise/cerise_backend.log .


This will copy the log file ``cerise_backend.log`` to your current directory,
where you can open it to have a look (it's plain text).

If you want to have a look around inside the running container, do

.. code-block:: bash

    docker exec -ti cerise-examples-test bash


If the test script has stopped or crashed, and the container is still running,
then you will want to stop the container and remove it, before rebuilding it and
trying again. You can do that using

.. code-block:: bash

    docker stop cerise-example-test
    docker rm cerise-example-test


Finally, if the service starts correctly, but something goes wrong with running
the workflow, then you can request the job log to get an error message. There's
another commented section in `test_script.py` showing how.

A more complex step
'''''''''''''''''''

Our second step will run a custom script that will be uploaded by Cerise. The
CWL step looks like this:

.. literalinclude:: ../examples/specialisation/api/example/steps/example/custom_program.cwl
  :caption: ``docs/examples/specialisation/api/example/steps/example/custom_program.cwl``


Note how it uses `$CERISE_PROJECT_FILES` to refer to the `files/` directory for
both the executable to be run, and for its first argument. The script itself is
in the `files/` directory of course. This is a plain bash script that
concatenates two files together:

.. literalinclude:: ../examples/specialisation/api/example/files/add_heading.sh
  :caption: ``docs/examples/specialisation/api/example/files/add_heading.sh``


Of course, you can put anything in here, including say compiled binaries for the
machine you're specialising for. When uploading, Cerise will copy permissions
along, so that executable files will remain executable and private files will
remain private. Unfortunately, there is a permission issue with Docker: when
copying your API into the Docker image, Docker will strip all permissions. In
the Dockerfile for the example, we manually make `add_heading.sh` executable
again, but for more complex sets of files this gets tedious to make and
maintain. In that case, it's probably better to create an archive from the
`api/` dir, and use the `ADD` command in the Dockerfile to extract it into the
container.

.. literalinclude:: ../examples/specialisation/Dockerfile
  :caption: ``docs/examples/specialisation/Dockerfile``


There's one thing missing in the above, the actual heading file, which
`add_heading.sh` expects to find at `$CERISE_PROJECT_FILES/heading.txt`. Of
course we could just put a `heading.txt` into the `files/` directory, but here
we have the install script create the heading file just to show how that works:

.. literalinclude:: ../examples/specialisation/api/example/install.sh
  :caption: ``docs/examples/specialisation/api/example/install.sh``


Since this is not a tutorial on shell programming, we keep it simple, printing a
one line message to the required file. Note that `$CERISE_PROJECT_FILES` is
automatically defined here. In practice, this script can be much more complex,
installing various libraries and programs as needed by your steps.

Alternatives for installing software
------------------------------------

If the program you want to run is available by default on the compute resource,
then running it is as simple as providing an appropriate CWL CommandLineTool
definition for it, as we did with `hostname`. Often however, you'll need to do a
bit or a lot of work to get there. There are at least four ways of making the
program you need available on the compute resource you are specialising for:

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
in beta however, and ``cwltiny`` (Cerise's internal runner) does not support it
at all yet.

Option 3 can work if you have enough permissions, but has the downside that the
existence of the installation will probably depend on the existence of your
account. If your account is deleted, your users' services will be stuck without
the software they need.

Option 4 is what we did above. It's not any more work than option 3, but makes
the installation independent from your involvement; if you put your
specialisation in a public version control repository, then anyone can
contribute. At the same time, you don't depend on external system
administrators' whims, or on your users having a lot of knowledge of HPC.

Versioning
----------

Your specialisation is effectively a library that gets used by the workflows
that your users submit. Like with any library, it is therefore a good idea to
put it into a version control system, and to give it a version number that
changes every time you change the steps, using `semantic versioning
<https://semver.org>`_. In fact, Cerise requires a version number.

If that sounds complicated and you want a simple way to get started that won't
cause problems in the future, just put your steps into
``myproject/steps/myproject/`` and put a single line ``0.0.0.dev`` in the
``myproject/version`` file. Whenever you add or change a step, increment the
second number in the ``version`` file by one (going from 0.9.0 to 0.10.0 and
beyond if necessary).

The ``.dev`` part will make Cerise reinstall your API every time it starts. Note
that that means that it will wait for running jobs to finish, reinstall the API,
then start running newly submitted jobs. This is very useful for development and
debugging, but not when you're running longer jobs, so in that case you will
want to remove the ``.dev`` postfix. If there is no ``.dev`` at the end of
the version number, Cerise will only reinstall if the version of the API on the
compute resource is lower than the local one, so be sure to increment the
version number if you make changes, otherwise you'll end up mixing different
versions, and that will probably end badly.

The only issue with this simple solution is that if you change a step in an
incompatible way (for example, when you change the name or the type of an input
or an output), your users' workflows will break. If it's just one or two people,
you can sit down with them and help them modify their workflows and then upgrade
everything at a single point in time, but if you have many users or many
workflows, then you have to either avoid this situation (by making new steps
rather than changing existing ones), or communicate it clearly.

If you're not the only one using your specialisation, then you should make sure
that they know what they can expect. Semantic Versioning is a standardised way
of doing this. A semantic version consists of three numbers, separated by
periods, and an optional postfix. The first number is incremented when an
incompatible change is made, i.e. one that may break things (workflows) that
depend on the versioned object (the steps). The second number is incremented
when new functionality is added in such a way that existing workflows keep
working, and the third number is incremented when there is no new functionality,
e.g. for bug fixes. The only postfix supported by Cerise is ``.dev``, as
described above. Furthermore, there is a general rule that for version numbers
starting with 0, anything goes, and there are no guarantees.

To use semantic versioning, put a notice in your documentation saying that
you're doing so, and whenever you make changes, update the version number
according to the above rules, both in the ``version`` file and in the
documentation. Now, if the users see that you've released a new major version
(e.g. 2.0.0), they'll know that their workflow may break.

If you want to be really fancy and expect your project to live for a long time
and have many users, then you can version your steps API. You do this by putting
your steps in a directory ``myproject/steps/myproject/1/step.cwl``. Now if you
want to make incompatible changes to your step, you can leave it in place, but
make a new version of it at ``myproject/steps/myproject/2/step.cwl``. As long as
you maintain the old versions, all workflows will keep working. If at some point
you want to stop supporting old steps, you can remove them, but be sure to
update your major version when you do so, because that can break existing
workflows.

Making a step template
----------------------

Once you have a specialisation with a few steps and an implementation for your
favourite compute resource, you may want to support other machines as well. To
do this, you'll need other specialisations, of the same project but for
different machines. Also, some way of keeping them in sync is a very good idea,
to ensure that any workflow designed for your project will run on any of the
specialisations.

The best way to do this is to make a step template. A step template is
basically just the steps directory of your specialisation, but containing
partially-defined steps. The steps are partially-defined because exactly
how a program is executed depends on the machine on which it's running, and
since the step template is the same for all specialisations, we don't know that
yet. What is important is that the inputs and outputs of each step are defined,
that there is a description of what it does exactly, and perhaps you can already
specify how to build the command line arguments from them.


The recommended layout of a step template is this::

    myproject
    ├── steps
    │   └── myproject
    │       ├── step1.cwl
    │       └── step2.cwl
    └── version

The ``version`` file will contain the version of the step template. This will be
a two-place version, ``major.minor``, where the major number is incremented when
there are incompatible changes to the step definitions, and the minor number is
incremented when there are compatible additions. There is no third number,
because there is no implementation to patch.

New specialisations can now start from this step template, and add a third
number to their version. Every time the implementation changes, but the step
definitions remain the same, only the third number is incremented. To change the
step definitoins, you change the template, increment its first or second number,
then update the specialisations to match, resetting them to ``x.y.0``, where
``x.y`` comes from the step template.

If you're now thinking that all this stuff is a bit complicated, well,
unfortunately, it can be. When you make a Cerise specialisation, you're making
software for others to use, and that's always a bit more complex than making
something only for yourself. On the other hand, if you have only a single
specialisation and set the version to ``0.0.0.dev``, then to ``0.1.0`` once
you're more or less done, then you can still use Cerise just fine by yourself
without ever thinking about versions. So how complex it gets depends on how many
features you want.

Remote execution
----------------

In the above example, we have set up Cerise to run in a Docker container, and to
execute jobs inside of the container. One way of using Cerise is to set it up
this way, then run the container on a compute server, and have users connect to
the REST API to submit and retrieve jobs. However, chances are you'll want to
use a compute cluster or supercomputer that you cannot just install software on.
In that case, it's better to run the Cerise container on your local machine, and
configure it to talk to the compute resource via the network. Here is an example
of such a configuration:

.. literalinclude:: ../examples/specialisation/config_remote.yml
  :caption: ``docs/examples/specialisation/config_remote.yml``
  :language: yaml


This configuration is for the DAS-5 supercomputer, a development system in The
Netherlands. We connect to it via SSH for starting jobs, and SFTP for file
transfer. Both protocols require a location to connect to. The remote path
contains another special string, $CERISE_USERNAME, which gets substituted by the
user name used to connect to the cluster. This way, each user will have their
own directory in their own home directory for Cerise to use. For submitting
jobs, the scheduler in use has to be specified, which is Slurm in case of the
DAS-5. Cerise also supports Torque/PBS, for which you should specify ``torque``.
There are some more options in this file, for which we refer to the
:doc:`configuration`.

One thing should be pointed out here though: while it's possible to put
credentials (e.g. usernames and passwords) in the configuration file, this is a
really bad idea for a public multi-user system such as the DAS-5. If you're
running your own cluster or compute server behind your firewall, and run a
single instance of the specialisation that connects to the compute resource
using a special account, then you can use that functionality, but for a public
machine, this is a really bad idea, and almost always against the terms of
service. Instead, every user should start their own instance of the
specialisation, which runs on their behalf, with their credentials. Those can be
injected into the Docker container via environment variables, and Cerise Client
will do this automatically. See the `Cerise Client documentation
<https://cerise-client.readthedocs.io>`_ for how to do
that.


.. _`CWL User Guide`: http://www.commonwl.org/v1.0/UserGuide.html

