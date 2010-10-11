""" Functions to interpret lists that get sent in as text"""

def parseRunList(l):
    """ Changes a string into a list of integers """
    toks = l.lstrip(' [').rstrip(' ]').split(',')
    if toks == ['']:
        return []
    return [int(tok) for tok in toks]

def parseBlockList(l):
    """ Changes a string into a list of strings """
    toks = l.lstrip(' [').rstrip(' ]').split(',')
    if toks == ['']:
        return []
    # only one set of quotes
    return [str(tok.strip(' \'"')) for tok in toks]

def parseSite(kw, name):
    """ puts site whitelist & blacklists into nice format"""
    value = kw.get(name, [])
    if value == None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value

