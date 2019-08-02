===========================
How to contribute to WMCore
===========================
Thank you for participating to WMCore!

* Please ensure that an `issue <https://github.com/dmwm/WMCore/issues/new/choose>`_ exists before submitting your contribution as a pull request.
  * There are two templates available to create a new issue, select the one matching your issue type.
* Pull request will only be merged if there is an associated issue (different solutions/implementations can be discussed on the issue).

A contribution can be either a **patch** or a **feature**:
 * **patch**: includes a bugfixes or an outstanding enhancement; besides going to the **master** branch, we also backport the same contribution to the latest **wmagent** branch.
 * **feature**: includes major developments or potentially disruptive changes and are included in feature releases made multiple times a year.

From the contribution types, we can also define at least two different branches:
 * **master**: it includes both features and patches contributions and it only reaches production when there is a CMSWEB/WMAgent upgrade.
 * **wmagent/crab/dbs**: it includes code and **patch** tags which are already deployed to production. Only patches/hotfixes make it to these branches.

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
However, please make sure you implement using the python 2.7 interpreter (having a virtualenv to switch between python2 and python3 is practically a must for the near future).
A non-exhaustive list of libraries which WMCore depend on can be found on the `requirements <https://github.com/dmwm/WMCore/blob/master/requirements.txt>`_ file.
Last but not least, please also have a look at the `Coding Style and checks` section below

Setting up the testing environment
----------------------------------

Perhaps even more important than setting up your development environment, is getting a neat testing environment.
You can find an extensive documentation on this `wiki_page <https://github.com/dmwm/WMCore/wiki/setup-wmcore-unittest>`_ on how to set it up and which options you have, however it's highly recommended to use the **docker** option such that you can have a development and testing environment closer to each other.

Contributing
------------

**Step 1**: Make sure there is already an `issue <https://github.com/dmwm/WMCore/issues/new/choose>`_ created, if not, then create one following one of the templates and providing all the necessary information.

**Step 2**: Create a local branch to start working on a proposal for that issue, branching off the "master" branch::

        git checkout -b your-branch-name origin/master


**Step 3**: implement your awesome feature/patch to fix the issue.

**Step 4**: Add and commit your change. The commit message must be the most **meaningful** possible, release notes are created from them (you can also amend it at a later stage, if needed)::

        git commit -m "here goes a very short and meaningful commit message"


**Step 5**: Now it's probably time to check the unit tests and:
 * make sure unit tests for the modules you touched are still succeeding
 * create new unit test(s)

**Step 6**: repeat the Step 4 to add and create a new commit. We **highly recommend** a separate commit for test-related changes like unit tests, emulation, json data,templates and so on.
In addition to unit tests, we ask you that any code refactoring **not changing any logical blocks**, as pylint, pep8 convention, fixing typos, etc; to be added to the same test commit.

**Step 7**: At this point you should have 2 commits in your branch: where the 1st commit contains real changes the proposed fix and; the 2nd commit contains aesthetic and unit tests changes.
Check the commits you have on your branch and then push your them to your forked repository::

        git log -10 --pretty=oneline --decorate
        git push origin your-branch-name

**Step 8**: then create a pull request either from your fork, or from the official github repository. There is a pull request template that you need to edit/update before confirming the pull request creation.
If you're proposing a **patch** that needs to be backported to a specific branch, please make sure to mention it in your pull request, such that the project responsible can properly label it.
The pull request title has to be meaningful as well, even though it's not used for the release notes. You might want to describe your changes and the reason behind that, it's quite helpful when we need to check a module's history.

**Step 9**: watch the pull request for comments and; if your pull request is ready to be reviewed, use the `Reviewers` option to ask a specific person(s) to review it.
If further changes are required to your pull request, please make sure to squash your commits in order to keep a clean commit history (remember, if you need to update both src/ and test/ files, then you need to squash them into the correct commits).

Automatic Tests
----------------

Every pull request - and further updates to that - trigger an automatic evaluation of your changes through our DMWM Jenkins infrastructure (only pull requests made against the **master** branch) and results are expected to come back within 30min.
This infrastructure is thoroughly described in this `wiki_section <https://github.com/dmwm/WMCore/wiki/Understanding-Jenkins>`_. However, in short there are 4 types of checks done by jenkins:

1. **unit tests**: all the WMCore unit tests are executed on top of your changes and compared against a master/HEAD baseline (which gets created twice a day). Besides unstable unit tests, your pull request will only be accepted once **all** unit tests succeed.

2. **pylint**: modules touched in your pull request get re-evaluated and a final score is given and compared against the upstream. Of course it's always supposed to increase, but it doesn't mean your pull request won't be accepted if it gets worse.
    * WMCore pylintrc is defined `here <https://github.com/dmwm/WMCore/blob/master/standards/.pylintrc>`_ and you should always pass this file when running pylint locally. The project follows the conventions described in there.
    * if you are proposing a brand new python module, then we expect it to have 0 pylint issues; if it's an older module - unless it's too much troublesome and dangerous - we always request to get the **E** and **W** pylint issues fixed (errors and warnings). Report type **C** and **R** are left for your consideration, if simple to fix in an IDE, then you should apply those changes and increase the code quality. If unsure, ask about it in the pull request.
    * reminder: any pylint updates are supposed to go with your 2nd commit, such that code review becomes easier.

3. **pycodestyle**: it corresponds to the pep8 checks and it should usually not report anything, these issues can be easily fixed by an IDE.

4. **python3 compatibility**: runs the futurize check to make pre-python 2.7 idions aren't reinserted in the code. We're currently using python 2.7 and trying to be as compatible as possible with python 3.

Human Review
------------

The submitter has to select someone from the WMCore team to review the pull request, even though anyone is welcome to review it and make comments!
Bare in mind that your pull request might not necessarily get approved, but further changes might be requested or even denied (and closed) to get into the base code.
Reviews are performed following the GitHub review mechanism, such that we can avoid many notifications for each comment made along the code.

Coding Style and checks
-----------------------

We use pep8 and pylint (including pylint3) to sanitize our code. Please do the same before submitting a pull request.
WMCore defines its own pylintrc `here <https://github.com/dmwm/WMCore/blob/master/standards/.pylintrc>`_, thus you should always pass this file as an argument when running pylint locally.

Extra documentation
-------------------

In case you're having issues with git and working through a branch feature, you might want to have a look at this old'ish `wiki <https://github.com/dmwm/WMCore/wiki/Developing-against-WMCore>`_ in our WMCore wiki documentation.
In addition to that, we've also compiled a long list of important git commands `here <https://github.com/dmwm/WMCore/wiki/git-commands>`_. If none of those work for you, google and stackoverflow will be your best friend.

