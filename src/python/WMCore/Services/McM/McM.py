#! /bin/env python

"""
A service class for retrieving data from McM using
an SSO cookie since it sits behind CERN SSO
"""

from builtins import str, object
import json
import os
import pycurl
import subprocess
import logging
from typing import List
from io import BytesIO
from WMCore.WMException import WMException


class McMNoDataError(WMException):
    """
    _McMNoDataError_
        McM responded but has no data for the request
    """

    def __init__(self):
        WMException.__init__(self, 'McM responded correctly but has no data')


class McM(object):
    """
    A service class for retrieving data from McM using
    an SSO cookie since it sits behind CERN SSO
    'key' must be unencrypted
    """

    def __init__(self, url='https://cms-pdmv.cern.ch/mcm', tmpDir='/tmp'):
        self.url = url
        self.tmpDir = tmpDir
        self.logger = self._get_logger()
        self.cookieFile = None

    def __enter__(self):
        self._krb_ticket()
        self._get_cookie()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.cookieFile:
            try:
                os.remove(self.cookieFile)
            except OSError:
                return
        return

    def _get_logger(self) -> logging.Logger:
        """
        Create a logger for McM client
        """
        logger: logging.Logger = logging.getLogger("mcm_client")
        date_format: str = "%Y-%m-%d %H:%M:%S %z"
        format: str = "[%(levelname)s][%(name)s][%(asctime)s]: %(message)s"
        formatter: logging.Formatter = logging.Formatter(fmt=format, datefmt=date_format)
        handler: logging.StreamHandler = logging.StreamHandler()

        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _krb_ticket(self) -> None:
        """
        Check there is a valid Kerberos ticket for requesting a SSO
        cookie. Raise a RuntimeError if there is not one.
        """
        process = subprocess.Popen(["klist", "-f"],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        stdout: str = process.communicate()[0].decode("utf-8")
        if process.returncode != 0:
            msg: str = ("There is no valid Kerberos ticket for requesting a SSO cookie. "
                        "Please make sure to provide one for your runtime environment"
                        )
            self.logger.error(msg)
            raise RuntimeError("FATAL -- %s\nError msg: %s" % (msg, stdout))

    def _get_cookie(self) -> None:
        """
        Request a SSO cookie to authenticate to McM.
        """
        self.cookieFile = os.path.join(self.tmpDir, 'ssoCookie.txt')
        command: List[str] = ["auth-get-sso-cookie", "-u", self.url, "-o", self.cookieFile, "-vv"]
        callback_not_invoked: str = "DEBUG: Not automatically redirected: trying SAML authentication"
        cookie_stored: str = "INFO: Saving cookies"

        process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, shell=False)
        stdout: str = process.communicate()[0].decode("utf-8")
        stdout_list: List[str] = stdout.strip().split("\n")
        if process.returncode != 0:
            raise RuntimeError("FATAL -- Error requesting SSO cookie\nError msg: %s" % (stdout))

        if callback_not_invoked in stdout:
            callback_idx: int = stdout_list.index(callback_not_invoked)
            stored_after_issue: bool = cookie_stored in stdout_list[callback_idx + 1]
            if stored_after_issue:
                msg: str = ("Callback method was not invoked. "
                            "Please make sure the provided Kerberos ticket is not linked to an account with 2FA"
                            )
                raise RuntimeError(msg)

    def _getURL(self, extendURL):
        """
        Fetch an MCM URL with CURL using the SSO cookie.
        Only intended to be used internally
        """

        try:
            b = BytesIO()
            c = pycurl.Curl()

            fullUrl = '%s/%s' % (self.url, extendURL)

            c.setopt(c.URL, fullUrl)
            c.setopt(c.SSL_VERIFYPEER, False)
            c.setopt(c.SSL_VERIFYHOST, False)
            c.setopt(c.FOLLOWLOCATION, True)
            c.setopt(c.COOKIEJAR, self.cookieFile)
            c.setopt(c.COOKIEFILE, self.cookieFile)
            c.setopt(c.WRITEFUNCTION, b.write)
            c.perform()
            if c.getinfo(pycurl.HTTP_CODE) != 200:
                raise IOError
        except:
            c.close()
            raise IOError('Was not able to fetch or decode URL from McM')

        try:
            body = b.getvalue()
            res = json.loads(body)
        except ValueError:
            c.close()
            raise IOError('Was not able to decode JSON from McM')

        c.close()
        return res

    def getHistory(self, prepID):
        """
        Get the history record which has who did what to an McM request
        """

        try:
            url = 'search?db_name=batches&contains=%s&get_raw' % prepID
            res = self._getURL(url)
            history = res.get("results", [])[0].get("history", [])
            return history
        except IndexError:
            raise McMNoDataError

    def getRequest(self, prepID):
        """
        Get the request record which has, among other things,
        the number of requested events
        """

        url = 'public/restapi/requests/get/%s' % prepID
        res = self._getURL(url)
        return res['results']


if __name__ == '__main__':
    with McM() as mcm:
        history = mcm.getHistory(prepID='BTV-Upg2023SHCAL14DR-00002')
        request = mcm.getRequest(prepID='BTV-Upg2023SHCAL14DR-00002')
