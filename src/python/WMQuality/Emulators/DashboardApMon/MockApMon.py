from __future__ import (division, print_function)
from builtins import object


class MockApMon(object):
    """
    Version of Services/PhEDEx intended to be used with mock or unittest.mock
    """
    def __init__(self, *args, **kwargs):
        print("Using MockApMon")

    def sendParams(self, params):
        """
        Send multiple parameters to MonALISA, with default(last given) cluser and node names.
        """
        return

    def sendTimedParams(self, timeStamp, params):
        """
        Send multiple parameters, specifying the time for them, with default(last given) cluster and node names.
        (See sendTimedParameters for more details)
        """
        return

    def sendParameter(self, clusterName, nodeName, paramName, paramValue):
        """
        Send a single parameter to MonALISA.
        """
        return

    def sendTimedParameter(self, clusterName, nodeName, timeStamp, paramName, paramValue):
        """
        Send a single parameter, with a given time.
        """
        return

    def sendParameters(self, clusterName, nodeName, params):
        """
        Send multiple parameters specifying cluster and node name for them
        """
        return

    def sendTimedParameters(self, clusterName, nodeName, timeStamp, params):
        """
        Send multiple monitored parameters to MonALISA.

        - clusterName is the name of the cluster being monitored. The first
          time this function is called, this paramenter must not be None. Then,
          it can be None; last given clusterName will be used instead.
        - nodeName is the name of the node for which are the parameters. If this
          is None, the full hostname of this machine will be sent instead.
        - timeStamp, if > 0, is given time for the parameters. This is in seconds from Epoch.
          Note that this option should be used only if you are sure about the time for the result.
          Otherwize, the parameters will be assigned a correct time(obtained from NTP servers)
          in MonALISA service. This option can be usefull when parsing logs, for example.
        - params is a dictionary containing pairs with:
            - key: parameter name
            - value: parameter value, either int or float.
          or params is a vector of tuples(key, value). This version can be used
          in case you want to send the parameters in a given order.

        NOTE that python doesn't know about 32-bit floats(only 64-bit floats!)
        """
        return

    def addJobToMonitor(self, pid, workDir, clusterName, nodeName):
        """
        Add a new job to monitor.
        """
        return

    def removeJobToMonitor(self, pid):
        """
        Remove a job from being monitored.
        """
        return

    def setMonitorClusterNode(self, clusterName, nodeName):
        """
        Set the cluster and node names where to send system related information.
        """
        return

    def enableBgMonitoring(self, onOff):
        """
        Enable or disable background monitoring. Note that background monitoring information
        can still be sent if user calls the sendBgMonitoring method.
        """
        return

    def sendBgMonitoring(self, mustSend=False):
        """
        Send background monitoring about system and jobs to all interested destinations.
        If mustSend == True, the information is sent regardles of the elapsed time since last sent
        If mustSend == False, the data is sent only if the required interval has passed since last sent
        """
        return

    def setDestinations(self, initValue):
        """
        Set the destinations of the ApMon instance. It accepts the same parameters as the constructor.
        """
        return

    def getConfig(self):
        """
        Returns a multi-line string that contains the configuration of ApMon. This string can
        be passed to the setDestination method(or to the constructor). It has the same
        structure as the config file/url contents.
        """

        return {}

    def initializedOK(self):
        """
        Returns true if there are destination(s) configured.
        """
        return True

    def freedOK(self):
        """
        Returns true if all ApMon resources were properly freed.
        """
        return True

    def setLogLevel(self, strLevel):
        """
        Change the log level. Given level is a string, one of 'FATAL', 'ERROR', 'WARNING',
        'INFO', 'NOTICE', 'DEBUG'.
        """
        return

    def setMaxMsgRate(self, rate):
        """
        Set the maximum number of messages that can be sent, per second.
        """
        return

    def setMaxMsgSize(self, size):
        """
        Set the maximum size of the sent messages. ApMon will try to split in several independent
        messages parameters sent in bulk, if the size would be larger than this
        """
        return

    def free(self):
        """
        Stop background threands, close opened sockets. You have to use this function if you want to
        free all the resources that ApMon takes, and allow it to be garbage-collected.
        """
        return
