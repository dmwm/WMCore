#!/usr/bin/env python
# -*- coding: utf-8 -*-
#pylint: disable=W0212
"""
File       : TokenManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: IAM token manager
             by default it relies on the following environment/parameters:
             - IAM_TOKEN is either file name or actual token value
             - https://cms-auth.web.cern.ch/jwk is default CMS IAM provider
             - https://wlcg.cern.ch/jwt/v1/any is default for audience parameter
"""

# system modules
import os
import ssl
import time
import logging
import traceback

# third part library
try:
    import jwt
except ImportError:
    traceback.print_exc()
    jwt = None

from Utils.Utilities import encodeUnicodeToBytes

# prevent "SSL: CERTIFICATE_VERIFY_FAILED" error
# this will cause pylint warning W0212, therefore we ignore it above
ssl._create_default_https_context = ssl._create_unverified_context


def readToken(name=None):
    """
    Read IAM token either from environment or file name
    :param name: ether file name containing token or environment name which hold the token value.
    If not provided it will be assumed to read token from IAM_TOKEN environment.
    :return: token or None
    """
    if name and os.path.exists(name):
        token = None
        with open(name, 'r', encoding='utf-8') as istream:
            token = istream.read()
        return token
    if name:
        return os.environ.get(name)
    return os.environ.get("IAM_TOKEN")


def tokenData(token, url="https://cms-auth.web.cern.ch/jwk", audUrl="https://wlcg.cern.ch/jwt/v1/any"):
    """
    inspect and extract token data
    :param token: token string
    :param url: IAM provider URL
    :param audUrl: audience string
    """
    if not token or not jwt:
        return {}
    if isinstance(token, str):
        token = encodeUnicodeToBytes(token)
    jwksClient = jwt.PyJWKClient(url)
    signingKey = jwksClient.get_signing_key_from_jwt(token)
    key = signingKey.key
    headers = jwt.get_unverified_header(token)
    alg = headers.get('alg', 'RS256')
    data = jwt.decode(
        token,
        key,
        algorithms=[alg],
        audience=audUrl,
        options={"verify_exp": True},
        )
    return data


def isValidToken(token):
    """
    check if given token is valid or not

    :param token: token string
    :return: true or false
    """
    tokenDict = {}
    tokenDict = tokenData(token)
    exp = tokenDict.get('exp', 0)  # expire, seconds since epoch
    if not exp or exp < time.time():
        return False
    return True


class TokenManager():
    """
    TokenManager class handles IAM tokens
    """

    def __init__(self,
                 name=None,
                 url="https://cms-auth.web.cern.ch/jwk",
                 audUrl="https://wlcg.cern.ch/jwt/v1/any",
                 logger=None):
        """
        Token manager reads IAM tokens either from file or env.
        It caches token along with expiration timestamp.
        By default the env variable to use is IAM_TOKEN.
        :param name: string representing either file or env where we should read token from
        :param url: IAM provider URL
        :param audUrl: audience string
        :param logger: logger object or none to use default one
        """
        self.name = name
        self.url = url
        self.audUrl = audUrl
        self.expire = 0
        self.token = None
        self.logger = logger if logger else logging.getLogger()
        try:
            self.token = self.getToken()
        except Exception as exc:
            self.logger.exception("Failed to get token. Details: %s", str(exc))

    def getToken(self):
        """
        Return valid token and sets its expire timestamp
        """
        if not self.token or not isValidToken(self.token):
            self.token = readToken(self.name)
        tokenDict = {}
        try:
            tokenDict = tokenData(self.token, url=self.url, audUrl=self.audUrl)
            self.logger.debug(tokenDict)
        except Exception as exc:
            self.logger.exception(str(exc))
            raise
        self.expire = tokenDict.get('exp', 0)
        return self.token

    def getLifetime(self):
        """
        Return reamaining lifetime of existing token
        """
        return self.expire - int(time.time())
