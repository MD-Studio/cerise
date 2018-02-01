Requirements
============

Introduction
------------

This document describes the requirements placed on Cerise.  The aim of this
project is to implement all these features. Most of them are currently there,
but Cerise is not yet completely done (and new features will probably keep
coming up).

Overview
--------

Cerise provides a REST interface through which CWL workflows can be submitted
for execution on a compute resource. The particulars of the compute resource are
configurable, with support for local execution as well as SSH-accessed remote
machines, and compute clusters using SLURM or TORQUE as their resource manager.
There will not be complete support for all possible CWL workflows.

The requirements below are categorised using the MoSCoW system: as either a
Must have, Should have, Could have or Won't have requirement.

Functionality
-------------

User side functionality
.......................

-   Job management

    -   [M] A user can submit a CWL workflow for execution using the
        `Netherlands eScience Center version of the GA4GH workflow execution
        schema`_ (the REST API).

    -   [M] Multiple workflows can be submitted and executing at the same time.

    -   [M] The service will execute the submitted workflow on a compute
        resource.

    -   [M] The status of the job, and eventual results of the workflow will be
        available through the REST API.

    -   [M] Running jobs can be cancelled (aborted).

    -   [M] Results may be left on the compute resource by copying them to a
        storage location on the compute resource using explicit commands that
        are part of the CWL workflow.

    -   [M] It must be easy to do the above things from a Python program.

    -   [W] Only a subset of CWL files will be supported. The exact subset is
        currently undefined, but custom input and output types will not be
        supported.

    -   [W] Inputs and outputs are small, on the order of megabytes, not
        gigabytes. The system does not have to support parallel up/downloads, or
        ones that take days to complete.


-   Workflow definition

    -   [M] The workflow will be defined in domain terms, not in terms of
        command line statements, core hours, or other low-level technical
        constructs. Different runs requiring different amounts of resources will
        be dealt with by offering steps for different scenarios, e.g. "run an
        LIE simulation efficiently using Gromacs for a protein in water" or "run
        an LIE simulation quickly using Gromacs for a protein in water" or "run
        a very long simulation of a protein using Gromacs". Thus, submitted
        workflow definitions are not compute resource specific.

    -   [M] Steps for specific scenarios can be configured into the software
        without changing the software itself.

    -   [M] It must be possible to specify multiple related files as an input
        (secondaryFiles in CWL terms), to have array-valued inputs (including
        arrays of files), and to have optional inputs and outputs (with default
        values for inputs).


Computing
.........

- Workflow execution

    -   [C] Serial execution of workflow steps is acceptable, with parallelism
        achieved through running many jobs. However, as there are typically
        limits to how many jobs can be submitted to a scheduler, parallel
        execution within a workflow would be nice to have.

    -   [C] On busy compute resources, queueing times can be long. Support for
        something like STOPOS (if available, such as on SURFsara Lisa) or pilot
        jobs could be considered.

- Configuration

    -   [M] Configurations may (or should) be compute-resource specific. We will
        not attempt grid-style resource abstraction, but instead rely on an
        administrator or developer to set up steps for each compute resource.

    -   [M] It must be possible to configure the service to select one compute
        resource on which submitted CWL workflows are to be run.

    -   [M] Given a desired set of workflow steps, it should be easy to
        configure these into the service. No superfluous hoops to jump through,
        and good documentation.

    -   [M] It should be possible to share configuration that is the same for
        different compute resources, to avoid duplication.

- Resource type support

    -   [M] Remote execution on a compute resource using either SLURM or
        TORQUE must be supported.

    -   [M] It must be possible to select specific resources within a cluster,
        e.g. GPUs, by submitting to a particular queue, or by specifying a
        resource constraint. These constraints are to be specified in a step.

    -   [S] Remote machines accessible through SSH should be supported

    -   [S] Local execution (on the machine the service runs on) should be
        supported.

    -   [S] With an eye towards the future, cloud resources should be supported.

    -   [C] With an eye towards the past, grid resources could be supported.

    -   [W] It would be nice to be able to distribute work across all available
        resources, but this is better done in a front-end accessing multiple
        resources, rather than by having a single service do both access and
        load balancing.


Deployment
..........

-   At least the following deployment configurations must be supported:

    -   [S] Client, this service, and workflow execution all on the same machine
    -   [S] Client on one machine, service and execution on another, where the
        client can connect to the service, but not vice versa.
    -   [M] Client, this service, and workflow execution each on a different
        machine or resource, where the client can connect to the service, and the
        service can connect to the compute resource, but no other connections are
        possible.

.. _`Netherlands eScience Center version of the GA4GH workflow execution schema`: https://github.com/NLeSC/workflow-execution-schemas
