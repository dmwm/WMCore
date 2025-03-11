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

def _baseProcessStatusFormat(pid=None):
    if not pid:
        pid = os.getpid()
    ttime = time.localtime()
    tstamp = time.strftime('%d/%b/%Y:%H:%M:%S', ttime)
    return {'pid': pid, 'timestamp': tstamp, 'time': time.time()}

def processStatus(pid=None):
    "Return status of the process in a dictionary format"

    pdict = _baseProcessStatusFormat(pid)
    if 'psutil' not in sys.modules:
        return pdict
    proc = psutil.Process(pid)
    pdict.update(proc.as_dict())
    return pdict

def processStatusDict(pid=None):
    "Return status of the process in a dictionary format"
    pdict = _baseProcessStatusFormat(pid)
    if 'psutil' not in sys.modules:
        return pdict
    proc = psutil.Process(pid)
    pdict.update({"cpu_times": dict(proc.cpu_times()._asdict())})
    pdict.update({"cpu_percent": proc.cpu_percent(interval=1.0)})
    pdict.update({"cpu_num": proc.cpu_num()})
    pdict.update({"memory_full_info": dict(proc.memory_full_info()._asdict())})
    pdict.update({"memory_percent": proc.memory_percent()})
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
    for tid, stack in list(sys._current_frames().items()):
        tdict = {"thead": id2name.get(tid, ""), "thead_id": tid}
        stacklist = []
        for filename, lineno, name, line in traceback.extract_stack(stack):
            sdict = dict(filename=filename, line_number=lineno, name=name, line=line)
            stacklist.append(sdict)
        tdict.update({"stack": stacklist})
        threads.append(tdict)
    return dict(threads=threads)

def processThreadsInfo(pid):
    """
    Provides information about process and its threads using psutils
    :param pid: process PID, string
    :return: dictionary statistics about process and threads
    """
    try:
        process = psutil.Process(pid)

        # Get overall process details
        process_info = {
            "process_name": process.name(),
            "pid": process.pid,
            "status": process.status(),
            "ppid": process.ppid(),  # Parent Process ID
            "cmdline": process.cmdline(),  # Full command-line arguments
            "cpu_usage_percent": process.cpu_percent(interval=1.0),  # CPU usage in percentage
            "memory_usage_percent": process.memory_percent(),  # RAM usage in percentage
            "memory_rss": process.memory_info().rss,  # Resident Set Size (bytes)
            "num_open_files": len(process.open_files()),  # Number of open file descriptors
            "num_connections": len(process.connections()),  # Number of active connections
            "threads": []
        }

        # Iterate over threads to get per-thread details
        for thread in process.threads():
            if str(pid) == str(thread.id):
                continue
            thread_id = thread.id

            thread_info = {
                "thread_id": thread_id,
                "user_time": thread.user_time,  # CPU time spent in user mode
                "system_time": thread.system_time,  # CPU time spent in system mode
                "cpu_usage_percent": None,
                "memory_usage_bytes": None,
                "num_open_files": None,
                "num_connections": None,
                "state": "unknown",
                "name": "thread"
            }
            if str(pid) == str(thread_id):
                thread_info['name'] = "process"

            # Try getting thread status
            try:
                thread_status = psutil.Process(thread_id).status()
                thread_info["state"] = thread_status
            except psutil.NoSuchProcess:
                thread_info["state"] = "zombie"

            # Try getting per-thread CPU usage
            try:
                thread_info["cpu_usage_percent"] = psutil.Process(thread_id).cpu_percent(interval=1.0)
            except Exception:
                thread_info["cpu_usage_percent"] = "N/A"

            # Try getting per-thread memory usage
            try:
                thread_info["memory_usage_bytes"] = psutil.Process(thread_id).memory_info().rss
            except Exception:
                thread_info["memory_usage_bytes"] = "N/A"

            # Try getting per-thread open file descriptors
            try:
                thread_info["num_open_files"] = len(psutil.Process(thread_id).open_files())
            except Exception:
                thread_info["num_open_files"] = "N/A"

            # Try getting per-thread open connections
            try:
                thread_info["num_connections"] = len(psutil.Process(thread_id).connections())
            except Exception:
                thread_info["num_connections"] = "N/A"

            # Append to process_info
            process_info["threads"].append(thread_info)

        return process_info
    except psutil.NoSuchProcess:
        return {"error": f"No process found with PID {pid}"}
    except Exception as e:
        return {"error": str(e)}

def main():
    "Main function to use this module as a stand-along script."
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument("--pid", action="store", dest="pid", help="process id")
    opts = parser.parse_args()

    pdict = processStatus(int(opts.pid))
    pdict.update(threadStack())
    print(f"Process status for {opts.pid}")
    print(json.dumps(pdict, indent=4))

    processInfo = processThreadsInfo(int(opts.pid))
    print(f"Process/threads status for {opts.pid}")
    print(json.dumps(processInfo, indent=4))

if __name__ == '__main__':
    main()
