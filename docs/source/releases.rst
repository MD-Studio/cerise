Releases
========

Cerise uses Git on GitHub for version management, using the `Git Flow`_
branching model. Making a release involves quite a few steps, so they're listed
here to help make the process more reliable. Cerise is not yet on PyPI, so for
now it's only a Git branch and a Docker image on DockerHub that are involved.

Make release branch
-------------------
To start the release process, make a release branch

.. code-block:: bash

  git checkout -b release-x.y.z develop

Cerise uses `Semantic Versioning`_, so name the new version accordingly.

Update version
--------------
Next, the version should be updated. There is a version tag in ``setup.py`` and
two for the documentation in ``docs/source/conf.py`` (search for ``version`` and
``release``). On the development branch, these should be set to ``develop``. On the
release branch, they should be set to ``x.y.z`` (or rather, the actual number of
this release of course).

Check documentation
-------------------
Since we've just changed the documentation build configuration, the buil should
be run locally to test:

.. code-block:: bash

  make docs

It may give some warnings about missing references; they should disappear if you
run the command a second time. Next, point your web browser to
``docs/build/index.html`` and verify that the documentation built correctly. In
particular, the new version number should be in the browser's title bar as well
as in the blue box on the top left of the page.

Run tests
---------
Before we make a commit, the tests should be run, and this is a good idea anyway
if we're making a release. So run ``make test`` and check that everything is
in order.

Commit the version update
-------------------------
That's easy:

.. code-block:: bash

  git commit -m 'Set release version'
  git push

This will trigger the Continuous Integration, so check that that's not giving
any errors while we're at it.

Merge into the master branch
----------------------------
If all seems to be well, then we can merge the release branch into the master
branch and tag it, thus making a release, at least as far as Git Flow is
concerned.

.. code-block:: bash

  git checkout master
  git merge --no-ff release-x.y.z
  git tag -a x.y.z
  git push


Add a Docker Hub build
----------------------
Finally, we need Docker Hub to build a properly tagged Docker image of the new
release. To get it do do this, follow these steps:

- Log in to Docker Hub
- Go to Organizations
- Go to ``mdstudio/cerise``
- Go to Build Settings
- Add a new Tag-based build with the ``x.y.z`` tag you just made

Next, pull the image to check that it works:

.. code-block:: bash

  docker pull mdstudio/cerise:x.y.z


.. _`Git Flow`: http://nvie.com/posts/a-successful-git-branching-model/
.. _`Semantic Versioning`: http://www.semver.org
