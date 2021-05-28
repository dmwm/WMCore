#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=
"""
File       : SummaryDB.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Set of modules/functions to update WMAgent SummaryDB.
"""

from future.utils import viewitems, viewvalues

# system modules
import logging
from pprint import pformat

from WMCore.Database.CMSCouch import CouchNotFoundError


def fwjr_parser(doc):
    """
    Parse FWJR document and yield the following structure:
    {"id": "requestName",
        "tasks": {
            "taskName1": {
                "sites": {
                    "siteName1":  {
                        "wrappedTotalJobTime": 1612,
                        "cmsRunCPUPerformance": {"totalJobCPU": 20, "totalJobTime": 42, "totalEventCPU": 4},
                        "inputEvents": 0,
                        "dataset": {}
                    }
                    "siteName2":  {
                        "wrappedTotalJobTime": 1612,
                        "cmsRunCPUPerformance": {"totalJobCPU": 20, "totalJobTime": 42, "totalEventCPU": 4},
                        "inputEvents": 0,
                        "dataset": {
                             "/a/b/c": {"size": 50, "events": 10, "totalLumis": 100},
                             "/a1/b1/c1": {"size": 50, "events": 10, "totalLumis": 100}
                        },
                    }
                },
            }
            "taskName2" : {}
        }
    }
    """
    # flag for the data collection, in some case fwjr doesn't contains the data needed
    # In that case it shouldn't crash but move on.
    dataCollectedFlag = False

    if 'fwjr' in doc:
        fwjr = doc['fwjr']
        if not isinstance(fwjr, dict):
            fwjr = fwjr.__to_json__(None)
    else:
        raise Exception('Document does not contain FWJR part')

    if 'task' not in fwjr:
        return dataCollectedFlag

    task_name = fwjr['task']
    try:
        req_name = task_name.split('/')[1]
    except:
        raise Exception('Cannot get request name from task_name "%s"' % task_name)

    # if one of the step has error except logArchive step don't collect the data
    for stepName in fwjr['steps']:
        if not stepName.startswith('logArch') and fwjr['steps'][stepName]['status'] != 0:
            return dataCollectedFlag

    sdict = {}  # all sites summary
    steps = fwjr['steps']
    wrappedTotalJobTime = 0
    pdict = dict(totalJobCPU=0, totalJobTime=0, totalEventCPU=0)
    ddict = {}
    for key, val in viewitems(steps):
        if val['stop'] is not None and val['start'] is not None:
            wrappedTotalJobTime += val['stop'] - val['start']
        site_name = val['site']
        site_summary = dict(wrappedTotalJobTime=wrappedTotalJobTime,
                            inputEvents=0, dataset=ddict,
                            cmsRunCPUPerformance=pdict)
        if key.startswith('cmsRun'):
            perf = val['performance']
            pdict['totalJobCPU'] += float(perf['cpu']['TotalJobCPU'])
            pdict['totalJobTime'] += float(perf['cpu']['TotalJobTime'])
            if 'TotalEventCPU' in perf['cpu']:
                pdict['totalEventCPU'] += float(perf['cpu']['TotalEventCPU'])
            else:
                pdict['totalEventCPU'] += float(perf['cpu']['TotalLoopCPU'])

            odict = val['output']
            for kkk, vvv in viewitems(odict):
                for row in vvv:
                    if row.get('merged', False):
                        prim = row['dataset']['primaryDataset']
                        proc = row['dataset']['processedDataset']
                        tier = row['dataset']['dataTier']
                        dataset = '/%s/%s/%s' % (prim, proc, tier)
                        totalLumis = sum([len(r) for r in viewvalues(row['runs'])])
                        dataset_summary = \
                            dict(size=row['size'], events=row['events'], totalLumis=totalLumis)
                        ddict[dataset] = dataset_summary
            if ddict:  # if we got dataset summary
                site_summary.update({'dataset': ddict})

            idict = val.get('input', {})
            if idict:
                source = idict.get('source', {})
                if isinstance(source, list):
                    for item in source:
                        site_summary['inputEvents'] += item.get('events', 0)
                else:
                    site_summary['inputEvents'] += source.get('events', 0)
        sdict[site_name] = site_summary
        dataCollectedFlag = True

    if dataCollectedFlag:
        # prepare final data structure
        sum_doc = dict(_id=req_name, tasks={task_name: dict(sites=sdict)})
        return sum_doc
    else:
        return dataCollectedFlag


def merge_docs(doc1, doc2):
    "Merge two summary documents use dict in place strategy"
    for key in ['_id', '_rev']:
        if key in doc1:
            del doc1[key]
        if key in doc2:
            del doc2[key]
    for key, val in viewitems(doc1):
        if key not in doc2:
            return False
        if isinstance(val, dict):
            if not merge_docs(doc1[key], doc2[key]):
                return False
        else:
            doc1[key] += doc2[key]
    return True


def update_tasks(old_tasks, new_tasks):
    "Update tasks dictionaries"
    for task, tval in viewitems(new_tasks):
        if task in old_tasks:
            for site, sval in viewitems(tval['sites']):
                if site in old_tasks[task]['sites']:
                    old_sdict = old_tasks[task]['sites'][site]
                    new_sdict = sval
                    status = merge_docs(old_sdict, new_sdict)
                    if not status:
                        logging.error("Error merge_docs:\n%s\n%s\n", pformat(old_sdict),
                                      pformat(new_sdict))
                else:
                    old_tasks[task]['sites'].update({site: sval})
        else:
            old_tasks.update({task: tval})
    return old_tasks


def updateSummaryDB(sumdb, document):
    "Update summary DB with given document"
    # parse input doc and create summary doc
    sum_doc = fwjr_parser(document)
    # check if DB has FWJR statistics
    if sum_doc is False:
        return False

    try:
        doc = sumdb.document(sum_doc['_id'])
        old_tasks = doc['tasks']
    except CouchNotFoundError:
        old_tasks = {}

    tasks = update_tasks(old_tasks, sum_doc['tasks'])

    try:
        combinedDoc = {'_id': sum_doc['_id'], 'tasks': tasks}
        resp = sumdb.updateDocument(sum_doc['_id'], "SummaryStats",
                                    "genericUpdate",
                                    fields=combinedDoc,
                                    useBody=True)
        # TODO: check response whether update successfull or not
        return True
    except Exception:
        logging.exception("Error fetching summary doc %s:", sum_doc['_id'])
        return False
