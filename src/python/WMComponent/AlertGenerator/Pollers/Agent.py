"""
Module encapsulates agent-related tests, e.g. CPU, memory utilisation
of agent's components, etc.

"""

# TODO
# Should consider ProcessDetail and Measurement occupying a common
# class - would that be a problem for other users of the current classes?
# For PeriodPoller? Things should then get a bit simpler, not to mention
# the order in which instances are now placed in two separate lists will
# be ensured automatically


import os
import logging
from xml.etree.ElementTree import ElementTree

from psutil import (NoSuchProcess, AccessDenied)
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
        # number of measurements (polling) before the values are evaluated
        # for possible alert sending (is set up in the _setUp method)
        self.numOfMeasurements = -1
        # list of instances of ProcessDetail class (processes (plus subprocesses)
        # that represent all (as read from the configuration) components of the agent
        self._components = []
        # list of instance of the Measurements class - measurements for 1 particular component
        self._compMeasurements = []
        self._setUp()


    def _getComponentsInfo(self):
        """
        Method returns dictionary[componentName] = componentPID
        from reading components' working directory.

        """
        result = {}
        et = ElementTree()
        components = self.agentCompleteConfig.listComponents_() + \
                     self.agentCompleteConfig.listWebapps_()
        for comp in components:
            compConfig = getattr(self.agentCompleteConfig, comp)
            daemonXml = os.path.join(compConfig.componentDir, "Daemon.xml")
            if not os.path.exists(daemonXml):
                logging.warn("%s: can't read file '%s' of component '%s', ignored." %
                              (self.__class__.__name__, daemonXml, comp))
                continue
            tree = et.parse(daemonXml)
            pid = None
            for child in tree.getchildren():
                if child.tag == "ProcessID":
                    pid = child.get("Value")
            if pid:
                result[comp] = pid # componentName, componentPID
        return result


    def _setUpProcessDetailAndMeasurements(self, compPID, compName):
        """
        Based on input, create instances of ProcessDetail and Measurements.

        """
        myName = self.__class__.__name__
        try:
            pd = ProcessDetail(compPID, compName)
            self._components.append(pd)
            self._compMeasurements.append(Measurements(self.numOfMeasurements))
            m = ("%s: loaded process information on %s:%s" % (myName, compName, compPID))
            logging.info(m)
        except (NoSuchProcess, AccessDenied) as ex:
            logging.error("%s: component %s ignored, reason: %s" % (myName, compName, ex))


    def _setUp(self):
        """
        First get the list of all components that constitute the agent.
        Check their work directories to load their Daemon.xml file and read
        their base process PID. Create corresponding instance of ProcessDetail
        and Measurements classes.

        """
        self.numOfMeasurements = round(self.config.period / self.config.pollInterval, 0)
        # list of pairs (componentPID, componentName)
        componentsInfo = self._getComponentsInfo()
        for compName, compPID in componentsInfo.items():
            self._setUpProcessDetailAndMeasurements(compPID, compName)


    def _updateComponentsInfo(self):
        """
        The method is called at each individual polling cycle.
        This handles:
            1) a particular component may have been restarted.
            2) some components were started after starting / initializing AlertGenerator
                itself so further running components (processes) may only be known later.

        """
        def removeItems(processDetail, measurements):
            self._components.remove(processDetail)
            self._compMeasurements.remove(measurements)

        myName = self.__class__.__name__
        # dictionary[componentName] = componentPID
        componentsInfo = self._getComponentsInfo()
        for processDetail, measurements in zip(self._components, self._compMeasurements):
            try:
                newPID = componentsInfo[processDetail.name]
                if int(newPID) == processDetail.pid:
                    # ok, component still runs under the same PID
                    # update list of child processes (some may have (dis)appeared)
                    logging.debug("Component %s runs under the same PID, refreshing"
                                  " list of child processes ..." % processDetail.getDetails())
                    try:
                        processDetail.refresh()
                    except NoSuchProcess as ex:
                        logging.error("Could not update list of children processes "
                                      "for %s, reason: %s" % (processDetail.getDetails(), ex))
                    del componentsInfo[processDetail.name]
                else:
                    logging.warn("Component %s seems to have been restarted "
                                 "(different PID:%s, was:%s)." % (processDetail.name,
                                 newPID, processDetail.pid))
                    try:
                        pd = ProcessDetail(newPID, processDetail.name)
                        index = self._components.index(processDetail)
                        self._components[index] = pd
                        measurements.clear()
                    except (NoSuchProcess, AccessDenied) as ex:
                        logging.error("%s: component %s ignored, reason: %s" % (myName, processDetail.name, ex))
                        removeItems(processDetail, measurements)
            except KeyError:
                m = "Component %s seems not running anymore, removed from polling." % processDetail.name
                logging.warning(m)
                removeItems(processDetail, measurements)

        if len(componentsInfo) > 0:
            logging.info("Some new components appeared since last check ...")
            for compName, compPID in componentsInfo.items():
                self._setUpProcessDetailAndMeasurements(compPID, compName)


    def check(self):
        self._updateComponentsInfo()
        for processDetail, measurements in zip(self._components, self._compMeasurements):
            try:
                PeriodPoller.check(self, processDetail, measurements)
            except NoSuchProcess as ex:
                logging.warn("Observed process or its child process(es) disappeared, "
                             "update at the next polling attempt, reason: %s." % ex)



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
