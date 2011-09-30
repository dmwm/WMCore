"""
Module encapsulates agent-related tests, e.g. CPU, memory utilisation
of agent's components, etc.

"""


import os
import logging
from xml.etree.ElementTree import ElementTree

import psutil

from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMComponent.AlertGenerator.Pollers.Base import PeriodPoller
from WMComponent.AlertGenerator.Pollers.System import ProcessCPUPoller
from WMComponent.AlertGenerator.Pollers.System import ProcessMemoryPoller



class ComponentsPoller(PeriodPoller):
    """
    The attributes get set up during initialization which is called
    from AlertGenerator constructor. Then the attributes are accessed
    from different process context which runs the periodic background
    polling.
    
    """
    def __init__(self, config, generator):
        PeriodPoller.__init__(self, config, generator)
        # access to the entire agent's configuration
        self.agentCompleteConfig = generator.config
        # list of instances of ProcessDetail class (processes (plus subprocesses)
        # that represent all (as read from the configuration) components of the agent
        self._components = []
        # list of instance of the Measurements class - measurements for 1 particular component
        self._compMeasurements = []
        self._setUp()
        
        
    def _setUp(self):
        """
        First get the list of all components that constitute the agent.
        Check their work directories to load their Daemon.xml file and read
        their base process PID. Create corresponding instance of ProcessDetail
        and Measurements classes.
        
        """
        myName = self.__class__.__name__
        numOfMeasurements = round(self.config.period / self.config.pollInterval, 0) 
        et = ElementTree()
        components = self.agentCompleteConfig.listComponents_() + \
                     self.agentCompleteConfig.listWebapps_()
        for comp in components:
            compConfig = getattr(self.agentCompleteConfig, comp)
            daemonXml = os.path.join(compConfig.componentDir, "Daemon.xml")
            if not os.path.exists(daemonXml):
                logging.error("%s: can't read file '%s' of component '%s', ignored" %
                              (myName, daemonXml, comp))
                continue
            tree = et.parse(daemonXml)
            pid = None
            for child in tree.getchildren():
                if child.tag == "ProcessID":
                    pid = child.get("Value")
            if pid:
                try:
                    pd = ProcessDetail(pid, comp)
                except (psutil.error.NoSuchProcess, psutil.error.AccessDenied), ex:
                    logging.error("%s: component %s ignored, reason: %s" % (myName, comp, ex))
                    continue
                self._components.append(pd)
                self._compMeasurements.append(Measurements(numOfMeasurements))
                m = ("%s: loaded process information on %s:%s" % (myName, comp, pid))
                logging.debug(m)
                    
                    
    def check(self):
        for processDetail, measurements in zip(self._components, self._compMeasurements):
            PeriodPoller.check(self, processDetail, measurements)


            
class ComponentsCPUPoller(ComponentsPoller):
    """
    Poller of CPU usage by the components of the agent (as found in the
    configuration).
    
    """
    
    def __init__(self, config, generator):
        ComponentsPoller.__init__(self, config, generator)
        
        
    @staticmethod
    def sample(processDetail):
        """
        Return a single float representing CPU usage of the main process
        and its subprocesses.
        
        """
        return ProcessCPUPoller.sample(processDetail)
                
        
        
class ComponentsMemoryPoller(ComponentsPoller):
    """
    Poller of the memory usage by the components of the agent (as found
    in the configuration).
    
    """
    def __init__(self, config, generator):
        ComponentsPoller.__init__(self, config, generator)
        
        
    @staticmethod
    def sample(processDetail):
        """
        Return a single float representing percentage usage of the main
        memory by the process.
        
        """        
        return ProcessMemoryPoller.sample(processDetail)