from __future__ import division, print_function
from builtins import str
import os


def getKeyCertFromEnv():
    """
    gets key and certificate from environment variables
    If no env variable is set return None, None for key, cert tuple

    First preference to HOST Certificate, This is how it set in Tier0

    """
    envPairs = [('X509_HOST_KEY', 'X509_HOST_CERT'),  # First preference to HOST Certificate,
                ('X509_USER_PROXY', 'X509_USER_PROXY'),  # Second preference to User Proxy, very common
                ('X509_USER_KEY', 'X509_USER_CERT')]  # Third preference to User Cert/Proxy combinition

    for keyEnv, certEnv in envPairs:
        key = os.environ.get(keyEnv)
        cert = os.environ.get(certEnv)
        if key and cert and os.path.exists(key) and os.path.exists(cert):
            # if it is found in env return key, cert
            return key, cert

    # TODO: only in linux, unix case, add other os case
    # look for proxy at default location /tmp/x509up_u$uid
    key = cert = '/tmp/x509up_u' + str(os.getuid())
    if os.path.exists(key):
        return key, cert

    # Finary look for globaus location
    if (os.environ.get('HOME') and
        os.path.exists(os.environ['HOME'] + '/.globus/usercert.pem')  and
        os.path.exists(os.environ['HOME'] + '/.globus/userkey.pem')):

        key = os.environ['HOME'] + '/.globus/userkey.pem'
        cert = os.environ['HOME'] + '/.globus/usercert.pem'
        return key, cert
    else:
        # couldn't find the key, cert files
        return None, None


def getCAPathFromEnv():
    """
    _getCAPathFromEnv_

    Return the path of the CA certificates. The check is loose in the pycurl_manager:
    is capath == None then the server identity is not verified. To enable this check
    you need to set either the X509_CERT_DIR variable or the cacert key of the request.
    """
    return os.environ.get("X509_CERT_DIR")
