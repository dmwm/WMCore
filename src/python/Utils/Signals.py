#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Signals.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: This module provide set of hooks to running program
The dumpthreads callback function will be called every time when
running program will receive SIGUSR1 signal. E.g., in unix shell
just do
shell# kill -s SIGUSR1 <pid>
"""

# system modules
import sys
import signal
import threading
import traceback

def dumpthreads(isignal, iframe):
    """
    Dump context of all threads upon given signal
    http://stackoverflow.com/questions/132058/showing-the-stack-trace-from-a-running-python-application
    """
    print("DAS stack, signal=%s, frame=%s" % (isignal, iframe))
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for tid, stack in list(sys._current_frames().items()):
        code.append("\n# Thread: %s(%d)" % (id2name.get(tid,""), tid))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if  line:
                code.append("  %s" % (line.strip()))
    print("\n".join(code))

signal.signal(signal.SIGUSR1, dumpthreads)
