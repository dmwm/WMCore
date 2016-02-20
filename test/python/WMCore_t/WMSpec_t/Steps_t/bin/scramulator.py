#!/usr/bin/env python
# encoding: utf-8
"""
scramulator.py

Scram emulator for unittests of the CMSSW runtime environments

Created by Dave Evans on 2010-03-15.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from __future__ import print_function

import sys
import os


def dispatcher(*args):
    """
    _dispatcher_

    Examine argument list and route to the appropriate call

    """
    if args[0] == '--arch':
        args = args[2:]

    if args[0] == 'project':
        scramProject(*args)
    elif args[0] == 'runtime':
        scramRuntime(*args)
    elif args[0] == 'ru':
        scramRuntime(*args)
    else:
        print("Unknown scram command: %s\nFull command: %s" % (args[0], args))
        sys.exit(1)


def scramProject(*args):
    print("Emulating scram project command...")
    workingDir = os.getcwd()
    projectVersion = args[2]
    newDir = os.path.join(os.getcwd(), projectVersion)
    if not os.path.exists(newDir):
        os.makedirs(newDir)

    sys.exit(0)


def scramRuntime(*args):
    print("export GREETING=\"Hello World\";")


if __name__ == '__main__':
    dispatcher(*sys.argv[1:])
