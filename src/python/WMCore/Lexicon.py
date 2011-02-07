#!/usr/bin/python
"""
_Lexicon_

A set of regular expressions  and other tests that we can use to validate input 
to other classes. If a test fails an AssertionError should be raised, and 
handled appropriately by the client methods, on success returns True. 
"""




import re

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
    pass

def cmsswversion(candidate):
    return check('CMSSW(_\d+){3}(_[a-z\d]+)?$', candidate)

def couchurl(candidate):
    return check('http://(0.0.0.0|localhost|(([a-zA-Z0-9:@\.\-_]){1,100})(fnal\.gov|cern\.ch)):\d+', candidate)

def check(regexp, candidate):
    assert re.compile(regexp).match(candidate) != None , \
              "'%s' does not match regular expression %s" % (candidate, regexp)
    return True
