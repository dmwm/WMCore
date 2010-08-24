#!/usr/bin/python
"""
_Lexicon_

A set of regular expressions  and other tests that we can use to validate input 
to other classes. If a test fails an AssertionError should be raised, and 
handled appropriately by the client methods, on success returns True. 
"""

__revision__ = "$Id: Lexicon.py,v 1.2 2009/02/03 17:49:44 metson Exp $"
__version__ = "$Revision: 1.2 $"

import re

def sitetier(candidate):
    return check("^T[0-3]", candidate)
    
def cmsname(candidate):
    return check("^T[0-3]_[A-Z]{2}_[A-Za-z]+", candidate)

def countrycode(candidate):
    #TODO: do properly
    return check("^[A-Z]{2}$", candidate)

def check(regexp, candidate):
    assert re.compile(regexp).match(candidate) != None , \
              "'%s' does not match regular expression %s" % (candidate, regexp)
    return True