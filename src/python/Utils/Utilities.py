#! /usr/bin/env python

from builtins import str, bytes

import subprocess
import os
import re
import zlib
import base64
import sys
from types import ModuleType, FunctionType
from gc import get_referents

import xml.etree.ElementTree as ET

def extractFromXML(xmlFile, xmlElement):
    tree = ET.parse(xmlFile)
    root = tree.getroot()
    element = root.find(f".//{xmlElement}")
    if element is not None:
        return element.get("Value")
    return None

def lowerCmsHeaders(headers):
    """
    Lower CMS headers in provided header's dict. The WMCore Authentication
    code check only cms headers in lower case, e.g. cms-xxx-yyy.
    """
    lheaders = {}
    for hkey, hval in list(headers.items()): # perform lower-case
        # lower header keys since we check lower-case in headers
        if hkey.startswith('Cms-') or hkey.startswith('CMS-'):
            lheaders[hkey.lower()] = hval
        else:
            lheaders[hkey] = hval
    return lheaders

def makeList(stringList):
    """
    _makeList_

    Make a python list out of a comma separated list of strings,
    throws a ValueError if the input is not well formed.
    If the stringList is already of type list, then return it untouched.
    """
    if isinstance(stringList, list):
        return stringList
    if isinstance(stringList, str):
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
    Try to convert different variations of True or False (including a string
    type object) to a boolean value.
    In short:
     * True gets mapped from: True, "True", "true", "TRUE".
     * False gets mapped from: False, "False", "false", "FALSE"
     * anything else will fail
    :param string: expects a boolean or a string, but it could be anything else
    :return: a boolean value, or raise an exception if value passed in is not supported
    """
    if string is False or string is True:
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
    output = df.communicate()[0]
    output = decodeBytesToUnicode(output).split("\n")
    for x in output:
        split = x.split()
        if split != [] and split[0] != 'Filesystem':
            diskPercent.append({'filesystem': split[0],
                                'mounted':    split[5], 
                                'percent':    split[4]})

    return diskPercent


def numberCouchProcess():
    """
    This returns the number of couch process
    """
    ps = subprocess.Popen(["ps", "-ef"], stdout=subprocess.PIPE)
    process = ps.communicate()[0]
    process = decodeBytesToUnicode(process).count('couchjs')

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


def zipEncodeStr(message, maxLen=5120, compressLevel=9, steps=100, truncateIndicator=" (...)"):
    """
    _zipEncodeStr_
    Utility to zip a string and encode it.
    If zipped encoded length is greater than maxLen,
    truncate message until zip/encoded version
    is within the limits allowed.
    """
    message = encodeUnicodeToBytes(message)
    encodedStr = zlib.compress(message, compressLevel)
    encodedStr = base64.b64encode(encodedStr)
    if len(encodedStr) < maxLen or maxLen == -1:
        return encodedStr

    compressRate = 1. * len(encodedStr) / len(base64.b64encode(message))

    # Estimate new length for message zip/encoded version
    # to be less than maxLen.
    # Also, append truncate indicator to message.
    truncateIndicator = encodeUnicodeToBytes(truncateIndicator)
    strLen = int((maxLen - len(truncateIndicator)) / compressRate)
    message = message[:strLen] + truncateIndicator

    encodedStr = zipEncodeStr(message, maxLen=-1)

    # If new length is not short enough, truncate
    # recursively by steps
    while len(encodedStr) > maxLen:
        message = message[:-steps - len(truncateIndicator)] + truncateIndicator
        encodedStr = zipEncodeStr(message, maxLen=-1)

    return encodedStr


def getSize(obj):
    """
    _getSize_

    Function to traverse an object and calculate its total size in bytes
    :param obj: a python object
    :return: an integer representing the total size of the object

    Code extracted from Stack Overflow:
    https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python
    """
    # Custom objects know their class.
    # Function objects seem to know way too much, including modules.
    # Exclude modules as well.
    BLACKLIST = type, ModuleType, FunctionType

    if isinstance(obj, BLACKLIST):
        raise TypeError('getSize() does not take argument of type: '+ str(type(obj)))
    seen_ids = set()
    size = 0
    objects = [obj]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = get_referents(*need_referents)
    return size


def decodeBytesToUnicode(value, errors="strict"):
    """
    Accepts an input "value" of generic type.

    If "value" is a string of type sequence of bytes (i.e. in py2 `str` or
    `future.types.newbytes.newbytes`, in py3 `bytes`), then it is converted to
    a sequence of unicode codepoints.

    This function is useful for cleaning input data when using the
    "unicode sandwich" approach, which involves converting bytes (i.e. strings
    of type sequence of bytes) to unicode (i.e. strings of type sequence of
    unicode codepoints, in py2 `unicode` or `future.types.newstr.newstr`,
    in py3 `str` ) as soon as possible when recieving input data, and
    converting unicode back to bytes as late as possible.
    achtung!:
    - converting unicode back to bytes is not covered by this function
    - converting unicode back to bytes is not always necessary. when in doubt,
      do not do it.
    Reference: https://nedbatchelder.com/text/unipain.html

    py2:
    - "errors" can be: "strict", "ignore", "replace",
    - ref: https://docs.python.org/2/howto/unicode.html#the-unicode-type
    py3:
    - "errors" can be: "strict", "ignore", "replace", "backslashreplace"
    - ref: https://docs.python.org/3/howto/unicode.html#the-string-type
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors)
    return value

def decodeBytesToUnicodeConditional(value, errors="ignore", condition=True):
    """
    if *condition*, then call decodeBytesToUnicode(*value*, *errors*),
    else return *value*
    
    This may be useful when we want to conditionally apply decodeBytesToUnicode,
    maintaining brevity.

    Parameters
    ----------
    value : any
        passed to decodeBytesToUnicode
    errors: str
        passed to decodeBytesToUnicode
    condition: boolean of object with attribute __bool__()
        if True, then we run decodeBytesToUnicode. Usually PY2/PY3
    """
    if condition:
        return decodeBytesToUnicode(value, errors)
    return value

def encodeUnicodeToBytes(value, errors="strict"):
    """
    Accepts an input "value" of generic type.

    If "value" is a string of type sequence of unicode (i.e. in py2 `unicode` or
    `future.types.newstr.newstr`, in py3 `str`), then it is converted to
    a sequence of bytes.

    This function is useful for encoding output data when using the
    "unicode sandwich" approach, which involves converting unicode (i.e. strings
    of type sequence of unicode codepoints) to bytes (i.e. strings of type
    sequence of bytes, in py2 `str` or `future.types.newbytes.newbytes`,
    in py3 `bytes`) as late as possible when passing a string to a third-party
    function that only accepts bytes as input (pycurl's curl.setop is an
    example).
    py2:
    - "errors" can be: "strict", "ignore", "replace", "xmlcharrefreplace"
    - ref: https://docs.python.org/2/howto/unicode.html#the-unicode-type
    py3:
    - "errors" can be: "strict", "ignore", "replace", "backslashreplace", 
      "xmlcharrefreplace", "namereplace"
    - ref: https://docs.python.org/3/howto/unicode.html#the-string-type
    """
    if isinstance(value, str):
        return value.encode("utf-8", errors)
    return value

def encodeUnicodeToBytesConditional(value, errors="ignore", condition=True):
    """
    if *condition*, then call encodeUnicodeToBytes(*value*, *errors*),
    else return *value*
    
    This may be useful when we want to conditionally apply encodeUnicodeToBytes,
    maintaining brevity.

    Parameters
    ----------
    value : any
        passed to encodeUnicodeToBytes
    errors: str
        passed to encodeUnicodeToBytes
    condition: boolean of object with attribute __bool__()
        if True, then we run encodeUnicodeToBytes. Usually PY2/PY3
    """
    if condition:
        return encodeUnicodeToBytes(value, errors)
    return value

def normalize_spaces(text):
    """
    Helper function to remove any number of empty spaces within given text and replace
    then with single space.
    :param text: string
    :return: normalized string
    """
    return re.sub(r'\s+', ' ', text).strip()
