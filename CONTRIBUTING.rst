===========================
How to contribute to WMCore
===========================
Thank you for participating in WMCore!

- Please ensure that a GitHub `issue <https://github.com/dmwm/WMCore/issues/new/choose>`_ exists before submitting your contribution through a pull request.

  - There are two templates available to create a new Github issue, select the one matching your issue type.
- Pull request will only be merged if there is an associated issue (different solutions/implementations can be discussed on the issue).

  - And at least one approval through the GitHub review process.

A contribution can be either a **patch** or a **feature**:

- **patch**: includes a bug-fixes or an outstanding enhancement; besides going to the **master** branch, we also backport the same contribution to the latest **wmagent** branch.
- **feature**: includes major developments or potentially disruptive changes and are included in feature releases following a monthly cycle.

From the contribution types, we can also define at least two different branches:

- **master**: it includes both features and patches contributions and it only reaches production when there is a CMSWEB/WMAgent upgrade.
- **wmagent/crab/dbs**: it includes code and **patch** tags which are already deployed to production. Only patches/hotfixes make it to these branches.

In general, we always start developing against the **master** branch, such that we can profit of the CI Jenkins infrastructure to evaluate and test our pull request.
In rare cases, we need to create a **temporary** fix that has to live only on the **wmagent** branch. If it's a temporary fix, it doesn't go to **master**.

Setting up the repository
-------------------------

**Step 1**: Fork the official `repository <https://github.com/dmwm/WMCore/>`_ on Github.

**Step 2**: Clone the repository to your development machine and configure it::

        git clone https://github.com/<YOUR_USER>/WMCore/
        cd WMCore
        git remote add upstream https://github.com/dmwm/WMCore.git


Setting up the development environment
--------------------------------------

There is no real recipe here, people use different operating systems and different IDE (Integrated Development Environment).
However, please make sure your dev environment defaults to python 3.8 interpreter.
A non-exhaustive list of libraries which WMCore depends on can be found on the `requirements <https://github.com/dmwm/WMCore/blob/master/requirements.txt>`_ file.
Last but not least, please also have a look at the `Coding Style and checks` section below

Setting up the testing environment
----------------------------------

Perhaps even more important than setting up your development environment, is getting a neat testing environment.
You can find an extensive documentation on this `wiki_page <https://github.com/dmwm/WMCore/wiki/setup-wmcore-unittest>`_ on how to set it up and which options you have, however it's highly recommended to use the **docker** option such that you can have a development and testing environment closer to each other.

Contributing
------------

**Step 1**: Make sure there is already an `issue <https://github.com/dmwm/WMCore/issues/new/choose>`_ created, if not, then create one according to the templates and providing all the necessary information. Note that there is text in the templates that you must replace by the description you are going to provide.

**Step 2**: Create a local branch to start working on a proposal for that issue, branching off the "master" branch::

        git fetch upstream
        git checkout -b your-branch-name upstream/master


**Step 3**: implement your awesome feature/patch to fix the issue.

**Step 4**: Add and commit your change. The commit message must be the most **meaningful** possible, release notes are created from them (you can also amend it at a later stage, if needed)::

        git commit -m "here goes a very short and meaningful commit message"


**Step 5**: Now it's probably time to check the unit tests and:

- make sure unit tests for the modules you touched are still succeeding
- create new unit test(s)

**Step 6**: repeat the Step 4 to add and create a new commit. We **highly recommend** a separate commit for test-related changes like unit tests, emulation, json data,templates and so on.
In addition to unit tests, we ask you that any code refactoring **not changing any logical blocks**, as pylint, pep8 convention, fixing typos, etc; to be added to the same test commit.

**Step 7**: At this point you should have 2 commits in your branch: where the 1st commit contains the real logic for your feature and/or bug-fix; and the 2nd commit contains aesthetic and unit tests changes.
Check the commits you have on your branch and then push them to your forked repository (amend commit messages if needed)::

        git log -10 --pretty=oneline --decorate
        git push origin your-branch-name

**Step 8**: then create a pull request either from your fork, or from the official github repository. There is a pull request template that you need to edit/update before actually creating your pull request:

- please make sure to provide a meaningful and short PR title;
- also provide a short description of the changes provided in the PR (if useful, also provide a reason for that decision and implementation)
- provide any possible dependency and/or external changes that are required to go with your pull request (kubernetes, COMP packages, deployments, etc).

If your pull request requires further effort and/or it's still a work in progress, please use one or both labels like: "Do not merge yet" and "Work in progress". If you're proposing a **patch** that needs to be backported to a specific branch and/or pushed to production right away, then please make sure to mention it in your pull request such that the proper actions and labelling are done.

**Step 9**: before asking for a pull request review, please make sure to provide any other material that is necessary to go together with your WMCore contribution (deployment, configurations, kubernetes, etc).
Once your pull request is ready to be reviewed, use the `Reviewers` option to ask a specific person(s) to review it. Watch your pull request for comments and feedback.
If further changes are required in your pull request, you might want to provide them in a separate commit, making it easier to review only the latest differences.

**Step 10**: when your pull request gets approved by at least one reviewer, you must squash your commits according to what has been explained above. In short:

- logical and algorithmical changes go in the first commit;
- while unit tests, pylint reformat, pep conventions, aesthetic minor improves go in the second commit.

Make sure to provide meaningful, short and free of typos commit messages, since they are used in our software release notes. If you think the commit message could be better, please amend it.
If you need help squashing your commits, please have a look at this short and clear document `this <https://steveklabnik.com/writing/how-to-squash-commits-in-a-github-pull-request>`_.

Timeframe expectations
-----------------------
People involved in the GH issues and pull requests should try commit to the following actions and their timeframes, such as:

- GH issues: tickets inactive for more than 3 years will be candidates to be closed out without any development.
- PR review: reviewers have 3 business days to provide feedback. If changes are too deep and/or complex, even a partial review is better than no review.
- PR follow up: we ask the developer/contributor to follow up on any required changes and/or questions with a one month time period. Otherwise the team might consider it no longer relevant and it could become eligible to be closed out.

Eventually we should integrate GH bots to start taking automated decisions based on some of these time period parameters.

Useful pull request labels
--------------------------

The WMCore project has many labels, however, here is a list of the most important labels for pull request contributions:

- ``Do not merge yet``: if changes are not fully read from your side, e.g. missing some extra validation, you might want to label your PR with this.
- ``Work in progress``: if you created a pull request for a development that you know it's still unfinished, please use this label.
- ``One approval required``: for somehow simple changes, or changes that are quite specific to a given WMCore service.
- ``Two approvals required``: for more complex changes; or changes that are more intrusive and need special attention.

Automatic Tests
----------------

Every pull request - and further updates made to it - trigger an automatic evaluation of your changes through our DMWM Jenkins infrastructure (only pull requests made against the **master** branch) and results are expected to come back within 20min.
This infrastructure is thoroughly described in this `wiki_section <https://github.com/dmwm/WMCore/wiki/Understanding-Jenkins>`_. However, in short there are 4 types of checks done by jenkins:

1. **unit tests**: all the WMCore unit tests are executed on top of your changes and compared against a master/HEAD baseline (which gets created twice a day). Besides unstable unit tests, your pull request will only be accepted once **all** unit tests succeed.

2. **pylint**: modules touched in your pull request get re-evaluated and a final score is given and compared against the upstream. Of course it's always supposed to increase, but it doesn't mean your pull request won't be accepted if it gets worse.

   - WMCore pylintrc is defined `here <https://github.com/dmwm/WMCore/blob/master/standards/.pylintrc>`_ and you should always pass this file when running pylint locally. The project follows the conventions described in there.
   - if you are proposing a brand new python module, then we expect it to have 0 pylint issues; if it's an older module - unless it's too much troublesome and dangerous - we always request to get the **E** and **W** pylint issues fixed (errors and warnings). Report type **C** and **R** are left for your consideration, if simple to fix in an IDE, then you should apply those changes and increase the code quality. If unsure, ask about it in the pull request.
   - reminder: any pylint updates are supposed to go with your 2nd commit, such that code review becomes easier.

3. **pycodestyle**: it corresponds to the pep8 checks and it should usually not report anything, these issues can be easily fixed by an IDE.

4. **python3 compatibility**: runs the futurize check to ensure that pre-python 2.7 idioms aren't reinserted in the code.

Code Review
------------

Every pull request has to be reviewed by at least one WMCore developer. Deeper and larger impact developments are ideally to be reviewed by 2 core developers. Nonetheless, others are encouraged to provide any feedback to such developments, regardless whether they have been marked as "Reviewers" or not. It also include cases where the contribution has already been merged.

Bare in mind that your pull request might not necessarily get approved and further changes could be requested. It's also possible that the team in the end decides not to accept those changes, providing you with a reason supporting that decision. Complex pull requests might go through partial reviews as well, which is better than no feedback at all.
Reviews are performed following the GitHub review mechanism, such that we can avoid many notifications for each comment made along the code. In general, reviewers will not pay too much attention to your pylint/pep/unittest changes (usually present in the 2nd commit), since those are supposed to be safe and not touching any algorithmically/logical parts of our baseline code.

A non-exhaustive **checklist for code review** is:

1. based on the Jenkins report: all unit tests need to be successful (exception for unstable tests)
2. based on the Jenkins report: there should be no **new** Errors and Warnings in pylint (exception for notifications from already existing code)
3. based on the Jenkins report: brand new modules need to be clean of any unit test/pylint/pycodestyle notifications
4. Pull Request no longer has the "PR: Do not merge yet" or "PR: Work in progress" labels
5. Commits have been properly squashed (usually 1 for src/* and 1 for test/* changes)
6. Commit messages are meaningful
7. Pull request initial description implements the template and has a clear description
8. Classes/methods/functions follow the docstring project recommendations.
9. Finally, does the pull request implement the expected behavior? Is the logic sound?
10. Does it require documentation? Has it been provided?


Creating unit tests
-----------------------

Test files need to be located under WMCore/test/python/ and
need to mirror the structure you use for the packages under
WMCore/src/python where every directory and test file is
augmented with a _t and the class should be augmented with 'Test'
If you develop for an external package you mimic this structure
in the external package directory structure.

E.g.: if you have a source package::

   src/python/WMCore/DataStructs/LumiList.py

you will have a test in the package::

   test/python/WMCore_t/DataStructs_t/LumiList_t.py

and in this module you would define a class named ``LumiListTest``, which inherits from ``unittest.TestCase``.

Your test class is supposed to implement the following methods:

- ``setUp``: this method implements a pre-setup for every single unit test (e.g., the database schema definition).
- ``tearDown``: this method implements a post-setup for every single unit test that gets executed, regardless of its exit code (e.g., cleaning up the database).
- ``test*``: methods starting with the ``test`` word are automatically executed by the unittest framework (and Jenkins).

Potential log files for tests should have a name: ``<testfile>.log``.
So in case of the ``LumiList_t.py`` you would get a ``LumiList_t.log`` file.

Coding Style and checks
-----------------------

We use pep8 and pylint to sanitize our code. Please do the same before submitting a pull request.
WMCore defines its own pylintrc `here <https://github.com/dmwm/WMCore/blob/master/standards/.pylintrc>`_ standards.
Thus, when evaluating your changes, please run pylint by passing this pylintrc file in the command line, your code should get scored 8 or above.
Unless there is a very good reason, we discourage the use of pylint disable statements.

Project Docstrings Best Practices
---------------------------------

With the goal of uniformizing and making the project more readable, we are adopting the `Sphinx` docstring style, which uses
reStructuredText markup. It's meant to document entire modules, classes, methods and functions. To avoid too large docstrings, we propose a subtle variation of this style such that each parameter
can be documented in a single line (instead of defining its type in a different line).

An example of a **good** single line docstring is::

    def printInfo(self):
        """Print information for this device."""

And another example of a **good** multi-line docstring is::

    def setDeviceName(self, devName):
        """
        Set the device name.

        :param devName: str, the name of the device to be defined.
        :return: bool, True if the operation succeeded, False otherwise
        """

Note that you are expected to provide docstrings following this style only if you are updating an existent one; or you have written one from scratch.
Otherwise, you do not need to update other docstrings that are unrelated to your changes.

Notes:

-  keep it as simple as possible (short summary, input parameters with data type, return data and data type)
-  use triple double-quotes, even if it's a single-line documentation
-  there is no blank line either before or after the docstring
-  special care and attention when documenting libraries shared across-projects and/or core modules

Further information can be found at the following links:

-  `PEP-257 <https://peps.python.org/pep-0257/>`_ specification, for docstring conventions.
-  `Sphinx <https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html#the-sphinx-docstring-format>`_ for Sphinx docstrings.
-  `reStructuredText <https://docutils.sourceforge.io/docs/user/rst/quickref.html>`_ for rst markup reference.


Extra documentation
-------------------

In case you're having issues with git and working through a branch feature, you might want to have a look at this old'ish `wiki <https://github.com/dmwm/WMCore/wiki/Developing-against-WMCore>`_ in our WMCore wiki documentation.
In addition to that, we've also compiled a long list of important git `commands <https://github.com/dmwm/WMCore/wiki/git-commands>`_. If none of those work for you, google and stackoverflow will be your best friend.


===========================
Structural and in-depth documentation
===========================


WMCore structure
-------------------

When developing utilitarian libraries that do **not** depend on any of the
WMCore libraries, create it under::

    src/python/Utils/

this package can be easily shared with external projects, so please ensure
it's well covered by unit tests and that its documentation (docstrings) are
as clean and clear as possible.

Core libraries, which can be shared among the several WMCore services, should
be implemented under::

    src/python/WMCore/

some of those are also shared with external projects.

When developing a new WMAgent component - which inherits from the ``Harness``
module, please use the following structure::

    src/python/WMComponent/<component name>/<component name.py>

and if it requires any specific config file, use::

    src/python/WMComponent/<component name>/DefaultConfig.py

WMCore/bin/wmcore-new-config is a config file aggregator that takes
as input directories roots and aggregates the config files. This enables
operators to generate one config file and edit it as they see fit. Beware that
if you import a DefaultConfig.py file in your DefaultConfig.py file this
can give errors when generating this file as it would overwrite existing
values.

If components contain parameters, those should be defined in the WMAgent default configuration/template file::

    etc/WMAgentConfig.py


On what concerns the tests, the module::

    src/python/WMQuality/TestInit.py

contains a set of methods often used in testing for performing mundane tasks,
such as setting up database connections, deploying/cleaning database schema, etc.
It is recommended that you use them to facilitate maintainability.

To facilitate using methods from the TestInit class for loading schemas, put
create statements in a ``...<backend>/Create.py`` method, following a similar
structure as can be found in the Create.py methods under ``./src/python/WMCore/Agent/Database/``.
Backend has either the value ``Oracle`` or ``MySQL``.

If you are creating a new WMCore package, or making changes for an external
project, verify whether your package - or an upper package - is listed under
the list of packages for each service, in the file::

    setup_dependencies.py

If not, then please add it whenever it's necessary. This is used by the package
builder for new releases.

Reserved words
-------------------

To prevent having to pass (a potential growing) number of parameters to
classes, there will be several reserved attributes in the thread class
to facilitate ease of use of much used objects. Below a list and how
you can assign them. These attributes enable (on a thread level) to
change values of certain often used objects (e.g. switching database
access parameters). It is not obligatory to use them just do not use
them in any other way than described here::

        import threading
        myThread = threading.currentThread()

        # pointer to the logger used in the module
        myThread.logger

        # pointer to current database interface object (WMCore.Database.DBInterface)
        myThread.dbi

        # the current database transaction object used (WMCore.Database.Transaction)
        myThread.transaction

        # A dictionary of factories. Factories are instantiated with a namespace
        # (e.g. WMCore.BossAir.MySQL) and load the appropriate objects. This is especially
        # useful if you work with multiple backends.
        myThread.factory

        # A String representing the backend. Currently there are 2: "MySQL", "Oracle".
        # These backends are used to define the proper namespace for importing data
        # access objects (DAO) from factories.
        # E.g. I can define a namespace: "WMCore.BossAir"+myThread.backend .
        myThread.backend

        # pointer to current message service object being used
        myThread.msgService

        # pointer to current trigger service object being used
        myThread.trigger

        # pointer to arguments used by this component
        myThread.args

        # dictionary of transactions. It is a - optional - possibility
        # to synchronize commits to multiple databases (or the same database)
        myThread.transactions
