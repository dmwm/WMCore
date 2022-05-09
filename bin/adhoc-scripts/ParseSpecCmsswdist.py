"""Script that parses a cmssw-dist spec files and returns a requirements.txt

Intro
=====

This script may have multiple purposes depending on the parameters it is given.

Non-recursive
=============

-d    Path of the spec's files directory (e.g. /path/to/cmssw-dist )
-f    Spec file filename (e.g. `wmagentpy3.spec`)

This is the default behavior. The script reads the specfile, parses the
``Requires:`` field selecting the ``py2-*.spec`` or ``py3-*.spec`` entries,
tries to guess the name of the package and adds it to the ``requirements.txt``
with the specified version.

Recursive
=========

The purpose of this behavior is to automatically check if the **python**
dependencies (in particular the ones gathered from pip)
of a package, that is built from a cmssw-dist spec file, support python3.

The script reads the specfile, parses the
``Requires:`` field selecting the ``py2-*.spec`` or ``py3-*.spec`` entries,
tries to guess the name of the package and adds it to the ``requirements.txt``
with the specified version. It then repeats the same search for every other
non-``py*-*.spec`` entry in the ``Requires:`` field recursively.

The result is a requirements.txt file that contains all the python
dependencies of a spec file. This can be used as an input for `caniusepython3`
in order to check if all the third-party libraries that a specfile depends on
support python3, since if a library supports python2 only it would stop the
py2->py2&py3 migration / modernization.

We considered spec files at https://github.com/cms-sw/cmsdist/tree/comp_gcc630 .

Step 1: Get the ``requirements.txt`` for a package, such as ``t0.spec``
-----------------------------------------------------------------------

-d              Path of the spec's files directory (e.g. /path/to/cmssw-dist )
-f              Spec file filename (e.g. `t0.spec`)
--recursive     Recursively scan imported spec files. (used to check if all the
                python2 dependencies support python3)

> BASEPATH=/path/to/local/github.com/cmsdist/
> SPECFILE=t0.spec
> python3 parse_spec_cmsswdist.py -d $BASEPATH -f $SPECFILE


``BASEPATH`` and ``SPECFILE`` are concatenated by ``os.path.join`` to get the path
of the specfile that we want to examine.

The output is saved in ``$PWD/requirements_$SPECFILE_auto.txt``

Step 2: How to interpret the results
------------------------------------

If a line of a ``requirements.txt`` contains a reference to a spec file, such
as

> docutils==0.12 # py2-docutils.spec

it means that a human should the correct line in the requirements file:

* if the package is in ``pypi``, just change the name.
  In the example of ``docutils``, it is available in pypi but it is downloaded
  from sourceforge.

* if the package is not in ``pypi``, comment the line.
  if you need to check if this package supports python3, do it manually
  and do not rely on ``caniusepython3``

Step 3: Analysie requirements.txt
---------------------------------

Install ``caniusepython3`` if you want to automatically check the
requirements.txt files.

> python3 -m pip install --user caniusepython3
>
> #Or run it insude a docker
> # on the host
> docker run -it -v $PWD:/src python:3.8 bash
> # inside the docker
> python3 -m pip install caniusepython3
> python3 /usr/local/bin/caniusepython3 --requirements /src/requirements_t0.spec.txt

ACHTUNG! These results are not complete!
1. ``caniusepython3`` has been designed to check only libraries distributed
  through pypi.org with pip. It can not check the libraries gathered from
  sourceforge or pythonhosted.
2. ``caniusepython3`` fails to identify some library, such as ``MySQL-python``,
  as a py2-only library, because the developers did not provide adequate
  python version trove classifiers.

TL;DR
-----

Use this tool with care: It can not provide any definitive answers,
but it can help identifying show-stoppers for the migration.

"""

from __future__ import division  # Jenkins CI

import argparse
import os
import logging
# import pprint
from pprint import pformat


DEPS_SPEC = set()


def getDepsSpec(specdir, specfile):
    """
    - Opens a spec files
    - Gets the list of Requirements
    - Gets the list of Requirements that start with `py2-` or `py3-`
    """
    specsPy = []  # p5- is for cpan, perl5 dependencies
    specsAll = []
    specfilePath = os.path.join(specdir, specfile)
    logging.info(specfilePath)
    with open(specfilePath, encoding='utf-8') as specFile:
        for line in specFile.readlines():
            requiresPattern = "Requires:"
            if line.strip().startswith(requiresPattern):
                deps = line[
                    line.find(requiresPattern) + len(requiresPattern):
                    ].strip()
                logging.debug(deps)
                specs = deps.split(" ")
                for spec in specs:
                    if "py2-" in spec or "py3-" in spec:
                        specsPy.append(spec.strip() + ".spec")
                    if spec.strip():
                        specsAll.append(spec.strip() + ".spec")
    return specsPy, specsAll


def getDepsRecursive(specdir, specfile, buildAllDep=False, recursionDepth=-1):
    """
    - calls getDepsSpec()
    - add all the dependencies that start with `py2-` or `py3-` to deps_spec
    - call recursively getDepsSpec() on all the dependencies
    :param specdir: The directory holding all the spec files to iterate through
    :param specfile: The initial spec file.
    :param buildAllDep: A Bool flag to signal building of all dependencies not only python related
    :param recursionDepth: An integer to set the maximum recursion depth.
    """

    global DEPS_SPEC
    # first time that we call this, no specfile is saved because
    # is it the parent specfile
    if recursionDepth == 0:
        return
    elif recursionDepth < 0:
        # if recursionDepth is negative means indefinite.
        pass
    else:
        recursionDepth -= 1

    specsPy, specsAll = getDepsSpec(specdir, specfile)
    logging.debug("%s: specsAll: \n%s", specfile, pformat(specsAll))
    logging.debug("%s: sepcsPy: \n%s", specfile, pformat(specsPy))
    if buildAllDep:
        DEPS_SPEC |= set(specsAll)
    else:
        DEPS_SPEC |= set(specsPy)

    for spec in specsAll:
        getDepsRecursive(specdir, spec, buildAllDep=buildAllDep, recursionDepth=recursionDepth)


def buildWithPip(lines):
    """
    Returns True if the files contains "build-with-pip", False otherwise
    """
    isBuildWithPip = False
    for line in lines:
        if "## IMPORT build-with-pip" in line:
            isBuildWithPip = True
    return isBuildWithPip


def getNameBuiltwithpip(lines):
    """
    Used if buildWithPip == True.
    If the file contains "%define pipname packagename", returns packagename.
    None otherwise
    """
    name = None
    for line in lines:
        if line.startswith("%define"):
            if "pip_name" in line:
                name = line.split()[-1]
    return name


def inPypi(lines):
    """
    Returns True if the package is hosted by pypi / pythonhosted.
    False otherwise
    """
    isInPypi = False
    for line in lines:
        if "Source" in line and ("pypi.python.org" in line or "files.pythonhosted.org" in line):
            isInPypi = True
    return isInPypi


def getNamePip(lines):
    """
    Used if inPypi == True.
    Extracts the pacakgename from lines such as
    "%setup -n Jinja2-%realversion", or "%define downloadn cx_Oracle".
    Returns None if known patterns are not matched
    """
    name = None
    for line in lines:
        if line.startswith("%setup"):
            name = line.split()[-1].split("-%")[0]
    if name and name.startswith("%"):
        for line in lines:
            if "%define " + name[1] in line:
                name = line.split()[-1]
    logging.debug(name)
    return name


def getPipVersion(specdir, specfile):
    """
    This function is intended to parse `py2-*.spec` and `py3-*.spec`
    spec files only
    """
    specpath = os.path.join(specdir, specfile)
    with open(specpath, encoding='utf-8') as specFile:
        lines = specFile.readlines()
        line0 = lines[0].strip()
        nameAndVer = []
        if "### RPM external" in line0:
            # nameAndVer = line0[line0.find("-") + 1:].split(" ")
            line0List = line0.split(" ")
            name=line0List[-2]
            # getting rid of py{2,3}- prefix in the package name.
            for patternStr in ("py2-", "py3-"):
                name = name.replace(patternStr, "")

            version=line0List[-1]
            nameAndVer = [name, version]
            logging.debug("%s %s", specfile, nameAndVer)
            if buildWithPip(lines):
                name = getNameBuiltwithpip(lines)
                name = name if name else nameAndVer[0]
                return "{0}=={1}".format(name, nameAndVer[1])
            if inPypi(lines):
                logging.debug(nameAndVer[0])
                return "{0}=={1}".format(getNamePip(lines), nameAndVer[1])
            return "{0}=={1} # {2} ".format(nameAndVer[0], nameAndVer[1], specfile)
    return None


def writeRequirements(specdir, depsspec, requirementsFilename):
    """Writes requirements.txt file"""
    requirementLines = []
    for spec in depsspec:
        # logging.info("finding pip version for spec: %s", spec)
        line = getPipVersion(specdir, spec)
        if line:
            line += "\n"
            requirementLines.append(line)
    with open(requirementsFilename, "w", encoding='utf-8') as requirementsFile:
        requirementsFile.writelines(requirementLines)


def main():
    """
    All parameters taken through argument parsing:
    :param --spec-dir: Path to base direcotry that contains the spec files
    :type spec-dir: str, required
    :param spec-file: filename of the spec file
    :type spec-file: str, required
    :param --level: Recursion level.
    :param --all: A Bool flag to signal for building all dependencies not python related.
    :param --recursive: A Bool flag to signal recursive iteration through spec files.
    """
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--spec-dir",
                        help="Path of the spec's files directory \
                              (e.g. `/path/to/cmssw-dist`)",
                        type=str,
                        required=True)
    parser.add_argument("-f", "--spec-file",
                        help="spec file filename (e.g. `t0.spec`)",
                        type=str,
                        required=True)
    parser.add_argument("-a", '--all', dest='buildAllDep', action='store_true', default=False)
    parser.add_argument("-l", '--level',
                        help="Recursion level. If --recursive is True and --level \
                              is specified recursion will stop at depth=level. Negative value means indefinite - This is the Default.",
                        dest='recursionDepth',
                        type=int,
                        default=-1)

    featureParser = parser.add_mutually_exclusive_group(required=False)
    featureParser.add_argument('--recursive', dest='recursive',
                               action='store_true',
                               help="Iterate recursively trough all spec files at SPEC_DIR.")
    featureParser.add_argument('--no-recursive', dest='recursive',
                               action='store_false',
                               help="Parse only the current spec file - This is the Default.")
    parser.set_defaults(feature=False)
    args = parser.parse_args()


    if args.recursive:
        getDepsRecursive(args.spec_dir, args.spec_file,
                         buildAllDep=args.buildAllDep,
                         recursionDepth=args.recursionDepth)
    else:
        getDepsRecursive(args.spec_dir, args.spec_file,
                         buildAllDep=args.buildAllDep,
                         recursionDepth=1)

    logging.info("DEPS_SPEC: len: %s: \n%s", len(DEPS_SPEC), pformat(DEPS_SPEC))

    writeRequirements(args.spec_dir, DEPS_SPEC,
                      "requirements_" + args.spec_file + "_auto.txt")


if __name__ == "__main__":
    main()
