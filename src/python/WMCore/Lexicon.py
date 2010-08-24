"""
A set of regular expressions that we can use to validate input to other classes. 
"""
import re

def sitetier(candidate):
    return check("^T[0-4]$", candidate)
    
def cmsname(candidate):
    return check("^T[0-4]_[A-Z]{2}_[A-Za-z]+", candidate)

def countrycode(candidate):
    #TODO: do properly
    return check("^[A-Z]{2}$", candidate)

def check(regexp, candidate):
    assert re.compile(regexp).match(candidate) == True, \
                  "'%s' does not match regular expression %s" % (candidate, regexp)
    return True