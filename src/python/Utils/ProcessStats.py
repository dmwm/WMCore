#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File       : ProcessStats.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: This module provide ability to get detailed information about
certain UNIX process, referred by PID. It is based on psutil and threading
python modules. It can be used in any web/data-server to get live status
about the running server. Here is an example

.. doctest:

    from Utils.procstats import status threadStack

    class SomeServer(object):
        def __init__(self):
        self.time0 = time.time()
        def status(self):
            sdict = {'server': processStatus()}
            sdict['server'].update({'uptime': time.time()-self.time0})
            sdict.update(threadStack())
            return sdict

The exposed status endpoint will return a json dictionary containing server
upteima and cpu/mem/threads information.
"""

# futures
from __future__ import print_function
from __future__ import division

# system modules
import os
import sys
import json
import time
import argparse
import threading
import traceback

try:
    import psutil
except ImportError:
    pass

def processStatus(pid=None):
    "Return status of the process in a dictionary format"
    if not pid:
        pid = os.getpid()
    ttime = time.localtime(time0) if time0 else time.localtime()
    tstamp = time.strftime('%d/%b/%Y:%H:%M:%S', ttime)
    pdict = {'pid': pid, 'timestamp': tstamp, 'time': time.time()}
    if 'psutil' not in sys.modules:
        return pdict
    proc = psutil.Process(pid)
    pdict.update(proc.as_dict())
    return pdict

def threadStack():
    """
    Return context of all threads in dictionary format where individual
    thread information stored in its own dicts and all threads are groupped
    into threads list. Code based on example from StackOverflow:
    http://stackoverflow.com/questions/132058/showing-the-stack-trace-from-a-running-python-application
    """
    tdict = {}
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    threads = []
    for tid, stack in sys._current_frames().items():
        tdict = {"thead": id2name.get(tid, ""), "thead_id": tid}
        stacklist = []
        for filename, lineno, name, line in traceback.extract_stack(stack):
            sdict = dict(filename=filename, line_number=lineno, name=name, line=line)
            stacklist.append(sdict)
        tdict.update({"stack": stacklist})
        threads.append(tdict)
    return dict(threads=threads)

def main():
    "Main function to use this module as a stand-along script."
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument("--pid", action="store", dest="pid", help="process id")
    opts = parser.parse_args()

    pdict = processStatus(int(opts.pid))
    pdict.update(threadStack())
    print(json.dumps(pdict))

if __name__ == '__main__':
    main()
