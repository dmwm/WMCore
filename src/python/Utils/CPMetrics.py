#!/usr/bin/env python3
"""
File       : CPMetrics.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: This module provide functions to flatten cherrypy stats
and provide them in Prometheus format for scraping.

Below we present three different outputs:
    - CherryPy stats default output
    - flatten structure of stats
    - prometheus output for stats

### Defautl CherryPy metrics output

{
    "Cheroot HTTPServer 4388603856": {
    "Accepts": 0,
    "Accepts/sec": 0.0,
    "Bind Address": "('127.0.0.1', 8080)",
    "Bytes Read": -1,
    "Bytes Written": -1,
    "Enabled": false,
    "Queue": 0,
    "Read Throughput": -1,
    "Requests": -1,
    "Run time": -1,
    "Socket Errors": 0,
    "Threads": 20,
    "Threads Idle": 19,
    "Work Time": -1,
    "Worker Threads": {
        "CP Server Thread-10": {
        "Bytes Read": 0,
        "Bytes Written": 0,
        "Read Throughput": 0.0,
        "Requests": 0,
        "Work Time": 0,
        "Write Throughput": 0.0
        },
        .....
    },
    "Write Throughput": -1
    },
    "CherryPy Applications": {
        "Bytes Read/Request": 0.0,
        "Bytes Read/Second": 0.0,
        "Bytes Written/Request": 0.0,
        "Bytes Written/Second": 0.0,
        "Current Requests": 0,
        "Current Time": 1601039589.916887,
        "Enabled": true,
        "Requests": {},
        "Requests/Second": 0.0,
        "Server Version": "17.4.2",
        "Start Time": 1601039576.541158,
        "Total Bytes Read": 0,
        "Total Bytes Written": 0,
        "Total Requests": 0,
        "Total Time": 0,
        "Uptime": 13.375718116760254
    }
}

### flatten structure via flattenStats function
{
"cherrypy_app_requests_second": 0.0,
"cherrypy_app_uptime": 1.7454230785369873,
"cherrypy_app_total_time": 0,
"cherrypy_app_current_time": 1601040713.369842,
"cherrypy_app_bytes_read_second": 0.0,
"cherrypy_app_requests": {},
"cherrypy_http_server_queue": 0,
"cherrypy_app_bytes_written_second": 0.0,
"cherrypy_app_total_requests": 0,
"cherrypy_http_server_write_throughput": -1,
"cherrypy_app_enabled": true,
"cherrypy_app_start_time": 1601040711.624408,
"cherrypy_http_server_threads": 20,
"cherrypy_http_server_work_time": -1,
"cherrypy_app_bytes_read_request": 0.0,
"cherrypy_http_server_bytes_read": -1,
"cherrypy_http_server_accepts_sec": 0.0,
"cherrypy_server_worker_threads": [
   {"thread_name": "cp_server_thread_3", "read_throughput": 0.0,
   "work_time": 0, "write_throughput": 0.0, "bytes_written": 0,
   "bytes_read": 0, "requests": 0}, ....],
"cherrypy_http_server_run_time": -1,
"cherrypy_http_server_bind_address": "('127.0.0.1', 8080)",
"cherrypy_app_server_version": "17.4.2",
"cherrypy_http_server_enabled": false,
"cherrypy_http_server_socket_errors": 0,
"cherrypy_app_bytes_written_request": 0.0,
"cherrypy_app_current_requests": 0,
"cherrypy_app_total_bytes_read": 0,
"cherrypy_http_server_bytes_written": -1,
"cherrypy_http_server_threads_idle": 19,
"cherrypy_http_server_requests": -1,
"cherrypy_app_total_bytes_written": 0,
"cherrypy_http_server_read_throughput": -1,
"cherrypy_http_server_accepts": 0}

### prometheus exporter structure provided via promMetrics function

# HELP cherrypy_app_requests_second
# TYPE cherrypy_app_requests_second gauge
cherrypy_app_requests_second 0.0
# HELP cherrypy_app_uptime
# TYPE cherrypy_app_uptime gauge
cherrypy_app_uptime 2.03766894341
# HELP cherrypy_app_total_time
# TYPE cherrypy_app_total_time counter
cherrypy_app_total_time 0
# HELP cherrypy_app_current_time
# TYPE cherrypy_app_current_time gauge
cherrypy_app_current_time 1601043839.47
# HELP cherrypy_app_bytes_read_second
# TYPE cherrypy_app_bytes_read_second gauge
cherrypy_app_bytes_read_second 0.0
# HELP cherrypy_http_server_queue
# TYPE cherrypy_http_server_queue counter
cherrypy_http_server_queue 0
# HELP cherrypy_app_bytes_written_second
# TYPE cherrypy_app_bytes_written_second gauge
cherrypy_app_bytes_written_second 0.0
# HELP cherrypy_app_total_requests
# TYPE cherrypy_app_total_requests counter
cherrypy_app_total_requests 0
# HELP cherrypy_http_server_write_throughput
# TYPE cherrypy_http_server_write_throughput counter
cherrypy_http_server_write_throughput -1
# HELP cherrypy_server_worker_threads
# TYPE cherrypy_server_worker_threads histogram
cherrypy_server_worker_threads{thread_name="cp_server_thread_3",...} 1
# HELP cherrypy_app_start_time
# TYPE cherrypy_app_start_time gauge
cherrypy_app_start_time 1601043837.44
# HELP cherrypy_http_server_threads
# TYPE cherrypy_http_server_threads counter
cherrypy_http_server_threads 20
# HELP cherrypy_http_server_work_time
# TYPE cherrypy_http_server_work_time counter
cherrypy_http_server_work_time -1
# HELP cherrypy_app_bytes_read_request
# TYPE cherrypy_app_bytes_read_request gauge
cherrypy_app_bytes_read_request 0.0
# HELP cherrypy_http_server_bytes_read
# TYPE cherrypy_http_server_bytes_read counter
cherrypy_http_server_bytes_read -1
# HELP cherrypy_http_server_accepts_sec
# TYPE cherrypy_http_server_accepts_sec gauge
cherrypy_http_server_accepts_sec 0.0
# HELP cherrypy_http_server_run_time
# TYPE cherrypy_http_server_run_time counter
cherrypy_http_server_run_time -1
# HELP cherrypy_http_server_socket_errors
# TYPE cherrypy_http_server_socket_errors counter
cherrypy_http_server_socket_errors 0
# HELP cherrypy_app_bytes_written_request
# TYPE cherrypy_app_bytes_written_request gauge
cherrypy_app_bytes_written_request 0.0
# HELP cherrypy_app_current_requests
# TYPE cherrypy_app_current_requests counter
cherrypy_app_current_requests 0
# HELP cherrypy_app_total_bytes_read
# TYPE cherrypy_app_total_bytes_read counter
cherrypy_app_total_bytes_read 0
# HELP cherrypy_http_server_bytes_written
# TYPE cherrypy_http_server_bytes_written counter
cherrypy_http_server_bytes_written -1
# HELP cherrypy_http_server_threads_idle
# TYPE cherrypy_http_server_threads_idle counter
cherrypy_http_server_threads_idle 19
# HELP cherrypy_http_server_requests
# TYPE cherrypy_http_server_requests counter
cherrypy_http_server_requests -1
# HELP cherrypy_app_total_bytes_written
# TYPE cherrypy_app_total_bytes_written counter
cherrypy_app_total_bytes_written 0
# HELP cherrypy_http_server_read_throughput
# TYPE cherrypy_http_server_read_throughput counter
cherrypy_http_server_read_throughput -1
# HELP cherrypy_http_server_accepts
# TYPE cherrypy_http_server_accepts counter
cherrypy_http_server_accepts 0
"""

# system modules
import json

# WMCore dependencies
from Utils.Utilities import decodeBytesToUnicode


def promMetrics(data, exporter):
    """
    Provide cherrypy stats prometheus metrics for given exporter name.
    """
    # exporter name should not contain dashes, see
    # https://its.cern.ch/jira/browse/CMSMONIT-514
    exporter = exporter.replace("-", "_")
    metrics = flattenStats(data)
    if isinstance(metrics, str):
        metrics = json.loads(metrics)
    # the following keys will be skipped
    skip = [
        'cherrypy_app_enabled',
        'cherrypy_http_server_bind_address',
        'cherrypy_app_requests',
        'cherrypy_app_server_version',
        'cherrypy_http_server_enabled']
    # our prometheus data representation
    pdata = ""
    for key, val in metrics.items():
        if key in skip:
            continue
        # add exporter name as a prefix for each key
        key = '{}_{}'.format(exporter, key)
        mhelp = "# HELP {}\n".format(key)
        if isinstance(val, list):
            mtype = "# TYPE {} histogram\n".format(key)
            pdata += mhelp
            pdata += mtype
            for wdict in val:
                entries = []
                for kkk, vvv in wdict.items():
                    entries.append('{}="{}"'.format(kkk, vvv))
                entry = "{%s}" % ','.join(entries)
                pdata += "{}{} 1\n".format(key, entry)
        elif isinstance(val, (str, tuple)):
            continue
        else:
            pdata += mhelp
            if isinstance(val, int):
                mtype = "# TYPE {} counter\n".format(key)
            if isinstance(val, float):
                mtype = "# TYPE {} gauge\n".format(key)
            pdata += mtype
            pdata += "{} {}\n".format(key, val)
    return pdata


def flattenStats(cpdata):
    "Flatten cherrypy stats to make them suitable for monitoring"
    if isinstance(cpdata, str) or isinstance(cpdata, bytes):
        cpdata = json.loads(decodeBytesToUnicode(cpdata))
    data = {}
    for cpKey, cpVal in cpdata.items():
        if cpKey.lower().find('cherrypy') != -1:
            for cpnKey, cpnVal in cpVal.items():
                nkey = 'cherrypy_app_%s' % cpnKey
                nkey = nkey.lower().replace(" ", "_").replace("/", "_")
                data[nkey] = cpnVal
        if cpKey.lower().find('cheroot') != -1:
            for cpnKey, cpnVal in cpVal.items():
                if cpnKey == 'Worker Threads':
                    wdata = []
                    for workerKey, threadValue in cpnVal.items():
                        workerKey = workerKey.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
                        threadValue['thread_name'] = workerKey
                        nval = {}
                        for tkey, tval in threadValue.items():
                            tkey = tkey.lower().replace(" ", "_").replace("/", "_")
                            nval[tkey] = tval
                        wdata.append(nval)
                    data["cherrypy_server_worker_threads"] = wdata
                else:
                    nkey = 'cherrypy_http_server_%s' % cpnKey
                    nkey = nkey.lower().replace(" ", "_").replace("/", "_")
                    data[nkey] = cpnVal
    return data
