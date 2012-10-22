"""
Class representation of an Alert message.
Representation of management messages within the Alert framework.

"""

import time


class Alert(dict):
    """
    Alert structure - alert message instance.

    """

    TEMPLATE = (u"Alert: Component: %(Component)s, Source: %(Source)s, "
                "Type: %(Type)s, Level: %(Level)s, Workload: %(Workload)s, "
                "HostName %(HostName)s, AgentName: %(AgentName)s, "
                "Timestamp: %(Timestamp)s, TimestampDecoded: %(TimestampDecoded)s, "
                "Details: %(Details)s")

    TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("Level", 0)
        self.setdefault("Source", None)
        self.setdefault("Type", None)
        self.setdefault("Workload", None)
        self.setdefault("Component", None)
        self.setdefault("Details", {})
        self.setdefault("Timestamp", None)
        # this is slightly redundant but it's convenient e.g. on an alert email
        # to see when the alert was generated without decoding the Timestamp
        self.setdefault("TimestampDecoded", None)
        # add a few values which are read from the configuration (Agent section)
        # (here are the first letters capitalised)
        self.setdefault("HostName", None)
        self.setdefault("Contact", None)
        self.setdefault("TeamName", None)
        self.setdefault("AgentName", None)
        self.update(args)


    level = property(lambda x: x.get("Level"))


    def setTimestamp(self):
        """
        Set time stamp attributes of a newly created alert instance.

        """
        t = time.time()
        timeStruct = time.gmtime(t) # convert time object to time struct
        self["Timestamp"] = t
        self["TimestampDecoded"] = time.strftime(self.TIMESTAMP_FORMAT, timeStruct)


    def toMsg(self):
        """
        Unlike e.g. __str__ which would return string representation
        of the dict instance, this method returns string ready for
        posting.

        """
        r = self.TEMPLATE % self
        return r



class RegisterMsg(dict):
    """
    Control message to register senders with Receiver instance.

    """

    key = u"Register"

    def __init__(self, label):
        dict.__init__(self)
        self[self.key] = label



class UnregisterMsg(dict):
    """
    Control message to unregister senders with Receiver instance.

    """

    key = u"Unregister"

    def __init__(self, label):
        dict.__init__(self)
        self[self.key] = label



class ShutdownMsg(dict):
    """
    Control message to shutdown the Receiver instance.

    """

    key = u"Shutdown"

    def __init__(self):
        dict.__init__(self)
        self[self.key] = True
