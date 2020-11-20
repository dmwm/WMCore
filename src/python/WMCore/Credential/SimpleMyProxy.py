#!/usr/bin/env python
#
# Originally from myproxy_logon script:
# http://www.mcs.anl.gov/fl/research/accessgrid/myproxy/myproxy_logon.py
# By Tom Uram <turam@mcs.anl.gov> on 2005/08/04
#
# Adapted/improved by
# Diego da Silva Gomes <diego@cern.ch>
# on 2013/04/08 for use by CMSWEB services.
#
# Added SimpleMyProxy class for WMCore.Credential module compatibility.
# Mattia Cinquilli <mcinquil@cern.ch>
# on 2013/04/14 for integration on CRAB services.

from builtins import str
import logging
import re
import socket

from OpenSSL import SSL, crypto

from WMCore.Credential.Credential import Credential

# we don't require password as we only want to allow x509 authentication
CMD_GET = """VERSION=MYPROXYv2
COMMAND=0
USERNAME=%s
PASSPHRASE=
LIFETIME=%d\0"""

CMD_INFO = """VERSION=MYPROXYv2
COMMAND=2
USERNAME=%s
PASSPHRASE=PASSPHRASE
LIFETIME=0\0"""


class MyProxyException(Exception):
    pass


def myproxy_ctx(certfile, keyfile):
    ctx = SSL.Context(SSL.SSLv3_METHOD)
    ctx.use_certificate_chain_file(certfile)
    ctx.use_privatekey_file(keyfile)

    # disable for compatibility with myproxy server (er, globus)
    # globus doesn't handle this case, apparently, and instead
    # chokes in proxy delegation code
    ctx.set_options(SSL.OP_DONT_INSERT_EMPTY_FRAGMENTS)

    return ctx


def create_cert_req(keyType=crypto.TYPE_RSA,
                    bits=1024,
                    messageDigest="md5"):
    """
    Create certificate request.

    Returns: certificate request PEM text, private key PEM text
    """

    # Create certificate request
    req = crypto.X509Req()

    # Generate private key
    pkey = crypto.PKey()
    pkey.generate_key(keyType, bits)

    req.set_pubkey(pkey)
    req.sign(pkey, messageDigest)

    return (crypto.dump_certificate_request(crypto.FILETYPE_ASN1, req),
            crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))


def deserialize_response(msg):
    m = re.search('RESPONSE=(\d)\n', msg)
    if m:
        resp = int(m.group(1))
    else:
        resp = 1  # set error if response not found

    errors = ", ".join(re.findall('^ERROR=(.*?)\n', msg, re.MULTILINE))
    data = "".join(re.findall('^(?!VERSION|RESPONSE|ERROR)(.*?\n)', msg, re.MULTILINE))
    return resp, errors, data


def deserialize_certs(inp_dat):
    pem_certs = []

    dat = inp_dat

    while dat:

        # find start of cert, get length
        ind = dat.find('\x30\x82')
        if ind < 0:
            break

        length = 256 * ord(dat[ind + 2]) + ord(dat[ind + 3])

        # extract der-format cert, and convert to pem
        c = dat[ind:ind + length + 4]
        x509 = crypto.load_certificate(crypto.FILETYPE_ASN1, c)
        pem_cert = crypto.dump_certificate(crypto.FILETYPE_PEM, x509)
        pem_certs.append(pem_cert)

        # trim cert from data
        dat = dat[ind + length + 4:]

    return pem_certs


def myproxy_client(sslctx, op, username, logger, lifetime=43200, host="myproxy.cern.ch", port=7512):
    """
    Function to info|get a proxy credential from a MyProxy server

    Exceptions: MyProxyException or any of the SSL exceptions
    """
    if op not in ['info', 'get']:
        raise MyProxyException('Wrong operation. Select "info" or "get".')

    logger.debug("debug: connect to myproxy server")
    conn = SSL.Connection(sslctx, socket.socket())
    conn.connect((host, port))

    logger.debug("debug: send globus compat byte")
    conn.write('0')

    logger.debug("debug: send the operation command")
    if op == 'info':
        cmd = CMD_INFO % username
    else:
        cmd = CMD_GET % (username, lifetime)
    conn.write(cmd)

    logger.debug("debug: process server response")
    d = conn.recv(8192)
    logger.debug(d)

    resp, error, data = deserialize_response(d)
    if resp:
        raise MyProxyException(error)
    logger.debug("debug: server response ok")

    if op == 'get':
        # The client will generate a public/private key pair and send a
        # NULL-terminated PKCS#10 certificate request to the server.
        logger.debug("debug: generate and send certificate request")
        certreq, privkey = create_cert_req()
        conn.send(certreq)

        logger.debug("debug: receive the number of certs")
        d = conn.recv(1)
        numcerts = ord(d[0])

        logger.debug("debug: receive %d certs" % numcerts)
        d = conn.recv(8192)

        logger.debug("debug: process server response")
        r = conn.recv(8192)
        resp, error, data = deserialize_response(r)
        if resp:
            raise MyProxyException(error)
        logger.debug("debug: server response ok")

        # deserialize certs from received cert data
        pem_certs = deserialize_certs(d)
        if len(pem_certs) != numcerts:
            raise MyProxyException("%d certs expected, %d received" % (numcerts, len(pem_certs)))
        logger.debug("debug: certs deserialized successfuly")

        # return proxy, the corresponding privkey, and then the rest of cert chain
        data = pem_certs[0] + privkey
        for c in pem_certs[1:]:
            data += c

    return data


class SimpleMyProxy(Credential):
    def __init__(self, args):
        Credential.__init__(self, args)
        self.logger = args['logger'] if 'logger' in args else logging.getLogger(type(self).__name__)

    def checkMyProxy(self, username, certfile=None, keyfile=None, myproxyserver='myproxy.cern.ch', myproxyport=7512):
        """
        Check if a valid myproxy exists and returns related info.
        This is supposed to be executed by the myproxy creator.
        """
        sslctx = myproxy_ctx(certfile if certfile else self.serverCert, keyfile if keyfile else self.serverKey)
        myproxy = myproxy_client(sslctx, 'info', username if username else self.userName, logger=self.logger,
                                 host=myproxyserver, port=myproxyport)
        myproxystatus = {}
        for line in myproxy.split('\n'):
            if line:
                if 'CRED_START_TIME' in line:
                    myproxystatus['start'] = line.split('CRED_START_TIME=')[1]
                elif 'CRED_END_TIME' in line:
                    myproxystatus['end'] = line.split('CRED_END_TIME=')[1]
                elif 'CRED_OWNER' in line:
                    myproxystatus['owner'] = line.split('CRED_OWNER=')[1]
                elif 'CRED_RETRIEVER_TRUSTED' in line:
                    myproxystatus['retriever'] = line.split('CRED_RETRIEVER_TRUSTED=')[1]
        return myproxystatus

    def logonRenewMyProxy(self, username, lifetime=43200, certfile=None, keyfile=None, myproxyserver='myproxy.cern.ch',
                          myproxyport=7512):
        """
        Retrieves a proxy from an existing myproxy server already delegated.
        """
        sslctx = myproxy_ctx(certfile if certfile else self.serverCert, keyfile if keyfile else self.serverKey)
        myproxy = myproxy_client(sslctx, 'get', username if username else self.userName, lifetime=lifetime,
                                 host=myproxyserver, port=myproxyport, logger=self.logger)
        return myproxy


if __name__ == '__main__':
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--op", dest="op",
                        help="Operation: info|get")
    parser.add_argument("-s", "--pshost", dest="host", default="myproxy.cern.ch",
                        help="The hostname of the MyProxy server to contact")
    parser.add_argument("-p", "--psport", dest="port", default=7512,
                        help="The port of the MyProxy server to contact")
    parser.add_argument("-l", "--username", dest="username",
                        help="The username with which the credential is stored on the MyProxy server")
    parser.add_argument("-C", "--certfile", dest="cert",
                        help="Certificate PEM filename to use with myproxy authentication")
    parser.add_argument("-y", "--keyfile", dest="key",
                        help="Certkey PEM filename to use with myproxy authentication")
    parser.add_argument("-t", "--proxy_lifetime", dest="lifetime", default=43200,
                        help="The lifetime validity for the fetched proxy.")
    parser.add_argument("-v", "--verbose", action="store_true", dest="debug", default=False,
                        help="Print debug information to stdout.")
    opt = parser.parse_args()


    def getLogging(debug):
        """Retrieves a logger and set the proper level

        :arg bool debug: it tells if needs a verbose logger
        :return logger: a logger with the appropriate logger level."""
        loglevel = logging.INFO
        if debug:
            loglevel = logging.DEBUG
        logging.basicConfig(level=loglevel)
        logger = logging.getLogger(__name__)
        logger.debug("Logging level initialized to %s.", loglevel)
        return logger


    logger = getLogging(debug=opt.debug)

    # process options
    if not opt.op or opt.op not in ['info', 'get']:
        logger.error("Error: incorrect operation, use --op (see -h for help)")
        sys.exit(1)
    if not opt.username:
        logger.error("Error: username not specified, use --username (see -h for help)")
        sys.exit(1)
    if not opt.cert or not opt.key:
        logger.error("Error: certificate/key not specified, use --certfile and --keyfile (see -h for help)")
        sys.exit(1)
    dbgfunc = lambda x: sys.stdout.write(x + '\n') if opt.debug else 0

    # Do the operation
    try:
        ctx = myproxy_ctx(opt.cert, opt.key)
        ret = myproxy_client(ctx, opt.op, opt.username, lifetime=int(opt.lifetime),
                             host=opt.host, port=int(opt.port), logger=logger)
        sys.stdout.write(ret)
    except Exception as e:
        if opt.debug:
            import traceback

            traceback.print_exc()
        logger.error("Error: %s", str(e))
        sys.exit(1)
