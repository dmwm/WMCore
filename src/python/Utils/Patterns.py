"""
Patterns module provides set of CS patterns
"""
import re

class Singleton(type):
    """Implementation of Singleton class"""
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = \
                    super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def getDomainName(urlStr):
    """
    Given a URL string, return the domain name.
    :param urlStr: URL string
    :return: a string with the domain name (e.g. "cmsweb-prod")
    """
    domainPattern = re.compile(r'https?://([^/]+)\.cern\.ch')
    match = domainPattern.search(urlStr)
    return match.group(1) if match else ""
