#!/usr/bin/env python

"""
This is the interface for Dashboard information submission

It's meant to be run after every job, parsing information out
of the job and the report.


"""
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()

from xml.dom import minidom
import logging
import traceback

import urllib.request, urllib.parse
import os
import socket
from contextlib import closing



from WMCore.WMSpec.WMWorkload import getWorkloadFromTask




USER_AGENT = \
"ProdMon/1.0 https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgentProdMon"



def addTextNode(document, parent, name, value):
    """
    _addTextNode_

    Add a text node with name and value to the parent node within the document

    """
    node = document.createElement(name)
    parent.appendChild(node)
    if value != None:
        text = document.createTextNode(str(value))
        node.appendChild(text)
    return

def HTTPpost(params, url, onFailureFile = None):
    """
    Do a http post with params to url

    params is a list of tuples of key,value pairs

    Taken directly from ProdAgent ProdMon DashboardInterface
    """

    try:
        logging.debug("contacting %s" % url)

        data = urllib.parse.urlencode(params)
        #put who we are in headers
        headers = { 'User-Agent' : USER_AGENT }
        req = urllib.request.Request(url, data, headers)

        with closing(urllib.request.urlopen(req, data)) as response:
            logging.debug("received http code: %s, message: %s, response: %s", response.code,
                          response.msg, str(response.read()))

    except IOError as ex:
        #record the report that failed then rethrow

        if onFailureFile != None:
            with open(onFailureFile, "w") as fd:
                fd.write(req.get_data())
            msg = str(ex)
            msg += "\nA copy of the failed report is in %s" % onFailureFile

        raise IOError(msg)

    return



class DashboardInterface(object):
    """
    Class to hold all functions related to dashboard information.

    Don't know offhand how necessary this is.
    """



    def __init__(self):
        """
        Put the job somewhere convenient

        """

        self.job       = None
        self.task      = None
        self.report    = None
        self.workload  = None
        self.agent     = None
        self.url       = None
        self.startTime = None
        self.endTime   = None
        self.info = {}
        self.document = minidom.Document()


        return


    def __call__(self, job, task, report, export = True, startTime = 'None', endTime = 'None'):
        """
        __call__

        Does everything.  Basically it runs all the other functions.

        Accepts three arguments, a DS Job object, a WMTaskHelper,
        and a FwkJobReport.Report

        """

        self.document = minidom.Document()  # This is the end product, the master report

        self.job       = job
        self.task      = task
        self.report    = report
        self.workload  = getWorkloadFromTask(task)
        self.startTime = startTime
        self.endTime   = endTime


        self.url      = getattr(task.data, 'dashboardURL',
                                "http://dashb-cmspa.cern.ch/dashboard/request.py/getPAinfo")

        self.createDocument()


        if export:
            self.exportDocument()


        return


    def createDocument(self):
        """
        Basically, the dashboard is moronic
        So you have to create a whole chain of
        XML objects that reference each other
        And then leave them empty
        """

        report = self.document.createElement("production_report")
        self.document.appendChild(report)
        team = self.document.createElement("team")
        team.setAttribute("name", "WMAgent")
        report.appendChild(team)
        agent = self.document.createElement("agent")
        agent.setAttribute("name", "WMAgent")
        team.appendChild(agent)

        self.createJobInfo(agent = agent)

        return




    def processInputFiles(self, job):
        """
        Put the input files (if any) in the document


        """


        files = job['input_files']


    def createJobInfo(self, agent):
        """
        Puts in the general job information

        """

        jobDoc = self.document.createElement("job")

        jobDoc.setAttribute("type", self.task.taskType())
        jobDoc.setAttribute("job_spec_id", str(self.job['id']))
        jobDoc.setAttribute("request_id", 'a1')
        jobDoc.setAttribute("workflow_name", 'name')

        # Add the input datasets
        input_datasets_node = self.document.createElement("input_datasets")
        jobDoc.appendChild(input_datasets_node)
        datasets = self.task.getInputDatasetPath()
        addTextNode(document = self.document,
                    parent = input_datasets_node,
                    name = "dataset",
                    value = str(self.task.getInputDatasetPath()))

        agent.appendChild(jobDoc)

        # Now add the output datasets from each step
        # Use output modules
        output_datasets_node = self.document.createElement("output_datasets")
        jobDoc.appendChild(output_datasets_node)
        outSteps = self.task.getOutputModulesForTask()
        for outMods in outSteps:
            for outMod in outMods:
                if outMod.listSections_() == []:
                    # Then we have an empty output Module
                    continue
                addTextNode(document = self.document,
                            parent = output_datasets_node,
                            name = "dataset",
                            value = '%s/%s/%s' % (outMod.primaryDataset,
                                                  outMod.processedDataset,
                                                  outMod.dataTier))


        # Create instance
        instance_node = self.document.createElement("instance")
        jobDoc.appendChild(instance_node)
        try:
            self.createInstanceDocument(instance = instance_node)
        except Exception as ex:
            msg = "Error while trying to create instances\n"
            msg += str(traceback.format_exc())
            msg += str(ex)
            logging.error(msg)
            raise Exception(ex)


        print(self.document.toprettyxml())

        return


    def createInstanceDocument(self, instance):
        """
        _createInstanceDocument_

        Put job info in an instance

        Which is not a job because it's super special
        """

        resource = self.document.createElement("resource")
        resource.setAttribute("site_name", str(self.job.get('locations', 'None')))
        resource.setAttribute("worker_node", str(socket.gethostname()))
        instance.appendChild(resource)


        # Add timing information
        addTextNode(self.document, instance,
                    "start_time", str(self.startTime))
        addTextNode(self.document, instance,
                    "end_time", str(self.endTime))
        addTextNode(self.document, instance,
                    "exit_code", str(self.report.taskSuccessful()))


        # Do output files and then set phedex node name (pnn)
        pnn = None
        output_node = self.document.createElement("output_files")
        instance.appendChild(output_node)
        outLFNs = []
        eventsWritten  = 0

        # Get output files
        for outfile in self.report.getAllFiles():
            pnn = outfile.get('location', 'None')
            eventsWritten += outfile.get('events', 0)
            addTextNode(self.document, output_node, "LFN",
                        str(outfile.get('lfn', 'None')))

        # Add PNN and events written
        resource.setAttribute("pnn", pnn)
        addTextNode(self.document, instance,
                    "events_written", str(eventsWritten))



        # Do input files
        eventsRead = 0
        input_node = self.document.createElement("input_files")
        instance.appendChild(input_node)
        for infile in self.report.getAllInputFiles():
            eventsRead = infile.get('events', 0)
            addTextNode(self.document, input_node, "LFN",
                        str(infile.get('lfn', 'None')))



        # Add job-wide variables
        addTextNode(self.document, instance,
                    "events_read", str(eventsWritten))



        # Empty nodes that do nothing
        timing_node = self.document.createElement("timings")
        instance.appendChild(timing_node)

        run_node = self.document.createElement("output_runs")
        instance.appendChild(run_node)

        skipped_node = self.document.createElement("skipped_events")
        instance.appendChild(skipped_node)

        return



    def exportDocument(self):
        """
        _exportDocument_

        Export document to Dashboard server
        """

        contents = [("report", self.document.toxml())]

        try:
            HTTPpost(contents, self.url,
                     onFailureFile = os.path.join(os.getcwd(), "Failed.txt"))
        except Exception as ex:
            msg = "Error exporting data to external monitoring: "
            msg += str(traceback.format_exc())
            msg += str(ex)
            logging.error(msg)


        logging.info("Export complete")

        return



    def getErrorCode(self):
        """
        _getErrorCode_

        Make up an error code.  I'm not sure what this is supposed to do
        so I'm just using the first error code I can come by.
        """

        value = ''

        stepName = self.task.getTopStepName()

        stepSection = self.report.retrieveStep(stepName)

        errorCount = getattr(stepSection.errors, "errorCount", 0)

        if errorCount == 0:
            return 'None'

        for errorNum in range(0, int(errorCount)):
            error = getattr(stepSection.errors, 'error%i' % (errorNum), None)
            if error:
                value += '%s:' % (str(getattr(error, 'exitCode', 0)))
