#!/usr/bin/env python
"""
Script to check the proxy validity and notify users in case it's close
to expiration.

Original author: direyes@cern.ch
"""
from __future__ import division, print_function

import getopt
import os
import re
import subprocess
import sys
import socket


HOST = socket.gethostname()


def main(argv):
    """
    This is an utility that checks the proxy validity and sends an email in
    case the time left is lesser than --time.
    Arguments:
    --proxy     : Proxy file location (Default: X509_USER_PROXY)
    --myproxy   : Check the log term proxy in myproxy (Default: True)
    --mail      : Mail where notification should be sent (Default: USER).
    --send-mail : <True|False> Send mail notification (Default: True).
    --time      : Minimun time left [hours]. It should be an integer. (Default: 48 h).
    --verbose   : Print output messages.
    --help      : Print this menu.

    Example: python /data/amaltaro/checkProxy.py --proxy /data/certs/myproxy.pem
    --time 72 --send-mail True --mail alanmalta@gmail.com,alan.malta@cern.ch --verbose
    """

    valid = ["proxy=", "myproxy=", "mail=", "send-mail=", "time=", "verbose", "help"]
    ### // Default values
    proxy = os.getenv('X509_USER_PROXY')
    myproxy = False
    verbose = False
    mail = os.getenv('USER')
    sendMail = True
    time = 3

    try:
        opts, _args = getopt.getopt(argv, "", valid)
    except getopt.GetoptError as ex:
        print("Options: {}\n\nException: {}".format(main.__doc__, str(ex)))
        sys.exit(1)

    ### // Handle arguments given in the command line
    for opt, arg in opts:
        if opt == "--help":
            print(main.__doc__)
            sys.exit(0)
        if opt == "--proxy":
            proxy = arg
            if proxy.startswith("~/"):
                proxy = os.getenv('HOME') + proxy[1:]
            if not os.path.exists(proxy):
                print("Proxy File does not exist")
                sys.exit(2)
        if opt == "--mail":
            mail = arg
        if opt == "--myproxy":
            myproxy = arg
        if opt == "--send-mail":
            sendMail = arg
        if opt == "--time":
            time = int(arg)
            if time < 1:
                print("Invalid time format. Check the options: {}".format(main.__doc__))
                raise sys.exit(3)
        if opt == "--verbose":
            verbose = True

    command = ["voms-proxy-info", "-file", str(proxy)]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, _err = p.communicate()
    proxyInfo = [line for line in out.split(b'\n') if line]
    processTimeLeft(sendMail, verbose, proxyInfo, time, mail)

    if myproxy:
        os.environ["X509_USER_CERT"] = proxy
        os.environ["X509_USER_KEY"] = proxy
        command = ["myproxy-info", "-v", "-l", "amaltaro", "-s", "myproxy.cern.ch", "-k", "amaltaroCERN"]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        proxyInfo = [line for line in out.split(b'\n') if line]
        processTimeLeft(sendMail, verbose, proxyInfo, time, mail)


def sendMailNotification(mail, message, proxyInfo='', verbose=False):
    if verbose:
        print("Host: {}".format(HOST))
    # append proxy info to message
    for line in proxyInfo:
        message += "%s\n" % line
    # echo %msg | mail -s 'HOST proxy status' MAIL_ADD
    command = " echo \"%s\" | " % (message)
    command += "mail -s '%s: Proxy status'" % (HOST)
    command += " %s" % (mail)

    if verbose:
        print("Running email command: {}".format(command))
    retCode = os.system(command)
    if verbose:
        print("Email notification exit code: {}".format(retCode))


def processTimeLeft(sendMail, verbose, proxyInfo, time, mail):
    """
    Receive the whole proxy info and return its time left.
    In case no proxy information is provided, it sends an
    email warning the user.
    """
    if proxyInfo:
        if verbose:
            print('Proxy information: {}'.format(proxyInfo))
        timeLeft = []
        for line in proxyInfo:
            if line.find(b'timeleft') > -1:
                dateReg = re.compile(b'\d{1,3}[:/]\d{2}[:/]\d{2}')
                timeLeft = dateReg.findall(line)[0]
                timeLeft = timeLeft.split(b':')[0]
                continue
    else:
        msg = "No valid proxy found in %s. " % HOST
        msg += "Please create one.\n"

        if verbose:
            print(msg)
            print("Send mail: {}".format(sendMail))

        if sendMail:
            if verbose:
                print("Sending mail notification")
            sendMailNotification(mail, msg)
        sys.exit(4)

        ### // build message
    if int(time) >= int(timeLeft):
        msg = "\nProxy file in %s is about to expire. " % HOST
        msg += "Please renew it.\n"
        msg += "Hours left: %i\n" % int(timeLeft)
        if int(timeLeft) == 0:
            msg = "Proxy file in %s HAS expired." % HOST
            msg += "Please renew it.\n"

        if verbose:
            print(msg)
            print("Send mail: {}".format(sendMail))

        ### // Sends an email
        if sendMail:
            if verbose:
                print("Sending mail notification")
            sendMailNotification(mail, msg, proxyInfo, verbose)


if __name__ == '__main__':
    main(sys.argv[1:])
