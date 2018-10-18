#! /usr/bin/env python

from __future__ import division, print_function

import subprocess
import os
import re


def makeList(stringList):
    """
    _makeList_

    Make a python list out of a comma separated list of strings,
    throws a ValueError if the input is not well formed.
    If the stringList is already of type list, then return it untouched.
    """
    if isinstance(stringList, list):
        return stringList
    if isinstance(stringList, basestring):
        toks = stringList.lstrip(' [').rstrip(' ]').split(',')
        if toks == ['']:
            return []
        return [str(tok.strip(' \'"')) for tok in toks]
    raise ValueError("Can't convert to list %s" % stringList)


def makeNonEmptyList(stringList):
    """
    _makeNonEmptyList_

    Given a string or a list of strings, return a non empty list of strings.
    Throws an exception in case the final list is empty or input data is not
    a string or a python list
    """
    finalList = makeList(stringList)
    if not finalList:
        raise ValueError("Input data cannot be an empty list %s" % stringList)
    return finalList


def strToBool(string):
    """
    _strToBool_

    Try to convert a string to boolean. i.e. "True" to python True
    """
    if string in [False, True]:
        return string
    elif string in ["True", "true", "TRUE"]:
        return True
    elif string in ["False", "false", "FALSE"]:
        return False
    raise ValueError("Can't convert to bool: %s" % string)


def safeStr(string):
    """
    _safeStr_

    Cast simple data (int, float, basestring) to string.
    """
    if not isinstance(string, (tuple, list, set, dict)):
        return str(string)
    raise ValueError("We're not supposed to convert %s to string." % string)


def diskUse():
    """
    This returns the % use of each disk partition
    """
    diskPercent = []
    df = subprocess.Popen(["df", "-klP"], stdout=subprocess.PIPE)
    output = df.communicate()[0].split("\n")
    for x in output:
        split = x.split()
        if split != [] and split[0] != 'Filesystem':
            diskPercent.append({'mounted': split[5], 'percent': split[4]})

    return diskPercent


def numberCouchProcess():
    """
    This returns the number of couch process
    """
    ps = subprocess.Popen(["ps", "-ef"], stdout=subprocess.PIPE)
    process = ps.communicate()[0].count('couchjs')

    return process


def rootUrlJoin(base, extend):
    """
    Adds a path element to the path within a ROOT url
    """
    if base:
        match = re.match("^root://([^/]+)/(.+)", base)
        if match:
            host = match.group(1)
            path = match.group(2)
            newpath = os.path.join(path, extend)
            newurl = "root://%s/%s" % (host, newpath)
            return newurl
    return None
