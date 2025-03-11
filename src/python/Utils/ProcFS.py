#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File       : ProcFS.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: This module provides function which rely on Linux proc fs
"""
# system modules
import os

# 3rd party modules
try:
    import psutil
    usePsutil = True
except ImportError:
    usePsutil = False


def processStatus(pid, method=None):
    """
    Return status of the given process PID along with its threads using psutil.
    :param pid: Process ID to inspect
    :param method: define which method to use procfs or psutil
    :return: list of dictionaries of `{process, pid, status}` data-structure
    """
    if method == 'procfs' or usePsutil == False:
        return processStatusViaProcFS(pid)
    return processStatusViaPsutil(pid)


def processStatusViaPsutil(pid):
    """
    Return status of the given process PID along with its threads using psutil.
    :param pid: Process ID to inspect
    :return: list of dictionaries with {process, pid, status, type}
    """
    statusList = []

    try:
        proc = psutil.Process(pid)
        processName = proc.name()
        processStatus = proc.status()
        statusList.append({
            "process": processName,
            "pid": str(pid),
            "status": processStatus,
            "type": "process"
        })

        # Iterate over threads
        threads = proc.threads()
        for thread in threads:
            if str(pid) == str(thread.id):
                continue
            threadId = thread.id
            # psutil doesn't provide thread name or detailed state, so we approximate
            statusList.append({
                "process": f"{processName}-thread",
                "pid": str(threadId),
                "status": "running",  # threads are assumed running if present
                "type": "thread"
            })

    except psutil.NoSuchProcess:
        return [{"error": f"Process {pid} not found"}]
    except Exception as e:
        return [{"error": str(e)}]

    return statusList


def processStatusViaProcFS(pid):
    """
    Return status of given process PID along with its threads via /proc FS look-up (available on all Linux OSes)
    :return: list of dictionaries of `{process, pid, status}` data-structure
    """
    statusList = []
    procPath = f"/proc/{pid}"

    if not os.path.exists(procPath):
        return [{"error": f"Process {pid} not found"}]

    try:
        with open(f"{procPath}/status", encoding='utf-8') as f:
            processName = ""
            processState = ""
            for line in f:
                if line.startswith("Name:"):
                    processName = line.split(":")[1].strip()
                elif line.startswith("State:"):
                    processState = line.split(":")[1].strip()
            statusList.append({"process": processName, "pid": str(pid), "status": processState, "type": "process"})

        taskPath = f"{procPath}/task"
        if os.path.exists(taskPath):
            for threadId in os.listdir(taskPath):
                if str(threadId) == str(pid):
                    continue
                with open(f"{taskPath}/{threadId}/status", encoding='utf-8') as f:
                    threadName = ""
                    threadState = ""
                    for line in f:
                        if line.startswith("Name:"):
                            threadName = line.split(":")[1].strip()
                        elif line.startswith("State:"):
                            threadState = line.split(":")[1].strip()
                    statusList.append({"process": threadName, "pid": threadId, "status": threadState, "type": "thread"})

    except Exception as e:
        return [{"error": str(e)}]

    return statusList
