#!/usr/bin/python
"""
_Lexicon_

A set of regular expressions  and other tests that we can use to validate input
to other classes. If a test fails an AssertionError should be raised, and
handled appropriately by the client methods, on success returns True.
"""

import re
import string

from WMCore.WMException import WMException

def sitetier(candidate):
    return check("^T[0-3]", candidate)

def cmsname(candidate):
    """
    Check candidate as a (partial) CMS name. Should pass:
        T2
        T2_UK
        T2_UK_SGrid
        T2_UK_SGrid_Bristol
    """
    #remove any trailing _'s
    candidate = candidate.rstrip('_')
    return check("^T[0-3%]((_[A-Z]{2}(_[A-Za-z]+)*)?)$", candidate)

def countrycode(candidate):
    #TODO: do properly with a look up table
    return check("^[A-Z]{2}$", candidate)

def block(candidate):
    pass

def identifier(candidate):
    """ letters, numbers, whitespace, periods, dashes, underscores """
    return check(r'[a-zA-Z0-9\s\.\-_]{1,100}$', candidate)

def dataset(candidate):
    """ A slash followed by an identifier,x3 """
    return check(r'(/[a-zA-Z0-9\.\-_]{1,100}){3}$', candidate)

def procdataset(candidate):
    pass

def primdataset(candidate):
    pass

def lfn(candidate):
    """
    Should be of the following form:
    /store/data/acquisition_era/primary-dataset/data_tier/processing_version/lfn_counter/filename.root
    See https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMPG_Namespace for details

    NOTE:Because of the way we do lustre, we have to have two separate checks for this:
    /store/data
    /store/data/lustre
    """
    regexp1 = '/([a-z]+)/([a-z]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)/([0-9]+)/([a-zA-Z0-9\-_]+).root'
    regexp2 = '/([a-z]+)/([a-z]+)/([a-z]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)/([0-9]+)/([a-zA-Z0-9\-_]+).root'
    try:
        return check(regexp1, candidate)
    except AssertionError:
        return check(regexp2, candidate)

def lfnBase(candidate):
    """
    As lfn above, but for doing the lfnBase
    i.e., for use in spec generation and parsing
    """
    regexp1 = '/([a-z]+)/([a-z]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)'
    regexp2 = '/([a-z]+)/([a-z]+)/([a-z]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)'
    try:
        return check(regexp1, candidate)
    except AssertionError:
        return check(regexp2, candidate)


def cmsswversion(candidate):
    return check('CMSSW(_\d+){3}(_[a-z\d]+)?$', candidate)

def couchurl(candidate):
    return check('http://(([a-zA-Z0-9:@\.\-_]){0,100})(localhost|fnal\.gov|cern\.ch|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d+', candidate)

def check(regexp, candidate):
    assert re.compile(regexp).match(candidate) != None , \
              "'%s' does not match regular expression %s" % (candidate, regexp)
    return True


def parseLFN(candidate):
    """
    _parseLFN_

    Take an LFN, return the component parts
    """

    # First, make sure what we've gotten is a real LFN
    lfn(candidate)

    parts = candidate.split('/')
    final = {}

    if parts[0] == '':
        parts.remove('')

    if len(parts) == 8:
        # Then we have only two locations
        final['baseLocation'] = '/%s' % string.join(parts[:2], '/')
        parts = parts[2:]
    elif len(parts) == 9:
        final['baseLocation'] = '/%s' % string.join(parts[:3], '/')
        parts = parts[3:]
    else:
        # How did we end up here?
        # Something just went wrong
        msg =  """CRITICAL!  This machine has experienced a complete logic failure while parsing LFNs.\n
        If you are a developer this indicates that you have changed the Lexicon LFN regexp functions without changing the parsing.\n
        If you are an operator, this indicates that this machine is likely unstable.\n
        All data should be backed up and the machine removed from production for examination.\n"""
        msg += "Candidate: %s" % candidate
        raise WMException(msg)


    final['acquisitionEra']    = parts[0]
    final['primaryDataset']    = parts[1]
    final['dataTier']          = parts[2]
    final['processingVersion'] = parts[3]
    final['lfnCounter']        = parts[4]
    final['filename']          = parts[5]

    return final


def parseLFNBase(candidate):
    """
    _parseLFNBase_

    Return a meaningful dictionary with info from an LFNBase
    """

    # First, make sure what we've gotten is a real LFNBase
    lfnBase(candidate)

    parts = candidate.split('/')
    final = {}

    if parts[0] == '':
        parts.remove('')

    if len(parts) == 6:
        # Then we have only two locations
        final['baseLocation'] = '/%s' % string.join(parts[:2], '/')
        parts = parts[2:]
    elif len(parts) == 7:
        final['baseLocation'] = '/%s' % string.join(parts[:3], '/')
        parts = parts[3:]
    else:
        # How did we end up here?
        # Something just went wrong
        msg =  """CRITICAL!  This machine has experienced a complete logic failure while parsing LFNs.\n
        If you are a developer this indicates that you have changed the Lexicon LFN regexp functions without changing the parsing.\n
        If you are an operator, this indicates that this machine is likely unstable.\n
        All data should be backed up and the machine removed from production for examination.\n"""
        msg += "Candidate: %s" % candidate
        raise WMException(msg)

    final['acquisitionEra']    = parts[0]
    final['primaryDataset']    = parts[1]
    final['dataTier']          = parts[2]
    final['processingVersion'] = parts[3]

    return final


