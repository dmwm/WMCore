"""
Helper script to send an Alert message directly to AlertProcesor for testing.

"""


import sys
import time

from WMCore.Alerts import API as alertAPI
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sender import Sender

machine = "maxatest.cern.ch"

target = "tcp://%s:6557" % machine
targetController = "tcp://%s:6559" % machine
if len(sys.argv) > 2:
    target = sys.argv[1]
    targetController = sys.argv[2]

dictAlert = dict(Type = "AlertTestClient", Workload = "n/a",
                Component = __name__, Source = __name__)
preAlert = alertAPI.getPredefinedAlert(**dictAlert)
sender = Sender(target, targetController, "AlertTestClient")
print ("created Sender client for alerts target: %s  controller: %s" %
        (target, targetController))


sender.register()
a = Alert(**preAlert)
a["Timestamp"] = time.time()
a["Level"] = 6
print "sending alert:\n'%s'" % a
sender(a)
sender.unregister()
