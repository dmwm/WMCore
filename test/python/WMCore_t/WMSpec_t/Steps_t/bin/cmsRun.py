#!/usr/bin/env python
# encoding: utf-8
"""
cmsRun.py

Created by Dave Evans on 2010-03-16.
Copyright (c) 2010 Fermilab. All rights reserved.

cmsRun simulator for unittest purposes.

If you are a physicist, use this instead of the real cmsRun and
complain that it doesnt make the events you ask for, I will kneecap you

"""

import sys

class CmsRun:
    """
    _CmsRun_

    Emulate cmsRun behaviour

    """
    def __init__(self, *args):
        self.args = list(args)
        open("BogusFile.txt", "w").close()

if __name__ == '__main__':
    cmsrun = CmsRun(*sys.argv[1:])
