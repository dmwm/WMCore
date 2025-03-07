"""

AlertManagerAPI - send alerts to MONIT AlertManager via API calls
"""

from __future__ import division
from builtins import object
from datetime import timedelta, datetime
import socket
import json
import logging

from WMCore.Services.pycurl_manager import RequestHandler
from Utils.Timers import LocalTimezone
from Utils.Utilities import normalize_spaces
from WMCore.Services.UUIDLib import makeUUID


class AlertManagerAPI(object):
    """
    A class used to send alerts via the MONIT AlertManager API
    """

    def __init__(self, alertManagerUrl, logger=None):
        self.alertManagerUrl = alertManagerUrl
        # sender's hostname is added as an annotation
        self.hostname = socket.gethostname()
        self.mgr = RequestHandler()
        self.ltz = LocalTimezone()
        self.headers = {"Content-Type": "application/json"}
        self.validSeverity = ["high", "medium", "low"]
        self.logger = logger if logger else logging.getLogger()

    def sendAlert(self, alertName, severity, summary, description, service, tag="wmcore", endSecs=600, generatorURL=""):
        """
        :param alertName: a unique name for the alert
        :param severity: low, medium, high
        :param summary: a short description of the alert
        :param description: a longer informational message with details about the alert
        :param service: the name of the service firing an alert
        :param tag: a unique tag used to help route the alert
        :param endSecs: how many minutes until the alarm is silenced
        :param generatorURL: this URL will be sent to AlertManager and configured as a clickable "Source" link in the web interface

        AlertManager JSON format reference: https://www.prometheus.io/docs/alerting/latest/clients/
        [
          {
            "labels": {
            "alertname": "<requiredAlertName>",
            "<labelname>": "<labelvalue>",
            ...
          },
            "annotations": {
              "<labelname>": "<labelvalue>",
            ...
          },
            "startsAt": "<rfc3339>", # optional, will be current time if not present
            "endsAt": "<rfc3339>",
            "generatorURL": "<generator_url>" # optional
          },
        ]
        """

        if not self._isValidSeverity(severity):
            return False

        request = []
        alert = {}
        labels = {}
        annotations = {}

        # add labels
        labels["alertname"] = alertName
        labels["severity"] = severity
        labels["tag"] = tag
        labels["service"] = service
        labels["uuid"] = makeUUID()
        alert["labels"] = labels

        # add annotations
        annotations["hostname"] = self.hostname
        annotations["summary"] = normalize_spaces(summary)
        annotations["description"] = normalize_spaces(description)
        alert["annotations"] = normalize_spaces(annotations)

        # In python3 we won't need the LocalTimezone class
        # Will change to d = datetime.now().astimezone() + timedelta(seconds=endSecs)
        d = datetime.now(self.ltz) + timedelta(seconds=endSecs)
        alert["endsAt"] = d.isoformat("T")
        alert["generatorURL"] = generatorURL

        request.append(alert)
        # need to do this because pycurl_manager only accepts dict and encoded strings type
        params = json.dumps(request)

        # provide dump of alert send to AM which will allow to match it in WM logs
        header, res = self.mgr.request(self.alertManagerUrl, params=params, headers=self.headers, verb='POST')
        self.logger.info("ALERT: name=%s UUID=%s, HTTP status code=%s", alertName, labels["uuid"], header.status)

        return res

    def _isValidSeverity(self, severity):
        """
        Used to check if the severity of the alert matches the valid levels: low, medium, high
        :param severity: severity of the alert
        :return: True or False
        """
        if severity not in self.validSeverity:
            logging.critical("Alert submitted to AlertManagerAPI with invalid severity: %s", severity)
            return False
        return True
