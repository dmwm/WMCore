#!/usr/bin/env python
"""
Basic interface to CERN ActiveMQ via stomp
"""
from __future__ import print_function
from __future__ import division

import json
import logging
import time
import uuid

import stomp

class StompyListener(object):
    """
    Auxiliar listener class to fetch all possible states in the Stomp
    connection.
    """
    def __init__(self):
        self.logr = logging.getLogger(__name__)

    def on_connecting(self, host_and_port):
        self.logr.info('on_connecting %s', str(host_and_port))

    def on_error(self, headers, message):
        self.logr.info('received an error %s %s', str(headers), str(message))

    def on_message(self, headers, body):
        self.logr.info('on_message %s %s', str(headers), str(body))

    def on_heartbeat(self):
        self.logr.info('on_heartbeat')

    def on_send(self, frame):
        self.logr.info('on_send HEADERS: %s, BODY: %s ...', str(frame.headers), str(frame.body)[:160])

    def on_connected(self, headers, body):
        self.logr.info('on_connected %s %s', str(headers), str(body))

    def on_disconnected(self):
        self.logr.info('on_disconnected')

    def on_heartbeat_timeout(self):
        self.logr.info('on_heartbeat_timeout')

    def on_before_message(self, headers, body):
        self.logr.info('on_before_message %s %s', str(headers), str(body))

        return (headers, body)


class StompAMQ(object):
    """
    Class to generate and send notifications to a given Stomp broker
    and a given topic.

    :param username: The username to connect to the broker.
    :param password: The password to connect to the broker.
    :param producer: The 'producer' field in the notification header
    :param topic: The topic to be used on the broker
    :param host_and_ports: The hosts and ports list of the brokers.
        E.g.: [('agileinf-mb.cern.ch', 61213)]
    """

    # Version number to be added in header
    _version = '0.1'

    def __init__(self, username, password,
                 producer='CMS_WMCore_StompAMQ',
                 topic='/topic/cms.jobmon.wmagent',
                 host_and_ports=None, verbose=0):
        self._host_and_ports = host_and_ports or [('agileinf-mb.cern.ch', 61213)]
        self._username = username
        self._password = password
        self._producer = producer
        self._topic = topic
        self.verbose = verbose

    def send(self, data):
        """
        Connect to the stomp host and send a single notification
        (or a list of notifications).

        :param data: Either a single notification (as returned by
            `make_notification`) or a list of such.

        :return: a list of successfully sent notification bodies
        """

        conn = stomp.Connection(host_and_ports=self._host_and_ports)
        conn.set_listener('StompyListener', StompyListener())
        try:
            conn.start()
            conn.connect(username=self._username, passcode=self._password, wait=True)
        except stomp.exception.ConnectFailedException as exc:
            print("ERROR: Connection to %s failed %s" % (repr(self._host_and_ports), str(exc)))
            return []

        # If only a single notification, put it in a list
        if isinstance(data, dict) and 'topic' in data:
            data = [data]

        successfully_sent = []
        for notification in data:
            body = self._send_single(conn, notification)
            if body:
                successfully_sent.append(body)

        if conn.is_connected():
            conn.disconnect()

        print('Sent %d docs to %s' % (len(successfully_sent), repr(self._host_and_ports)))
        return successfully_sent

    def _send_single(self, conn, notification):
        """
        Send a single notification to `conn`

        :param conn: An already connected stomp.Connection
        :param notification: A dictionary as returned by `make_notification`

        :return: The notification body in case of success, or else None
        """
        try:
            body = notification.pop('body')
            destination = notification.pop('topic')
            conn.send(destination=destination,
                      headers=notification,
                      body=json.dumps(body),
                      ack='auto')
            if  self.verbose:
                print('Notification %s sent' % str(notification))
            return body
        except Exception as exc:
            print('ERROR: Notification: %s not send, error: %s' % \
                          (str(notification), str(exc)))
            return None


    def make_notification(self, payload, id_, producer=None):
        """
        Generate a notification with the specified data

        :param payload: Actual notification data.
        :param id_: Id representing the notification.
        :param producer: The notification producer.
            Default: StompAMQ._producer

        :return: the generated notification
        """
        producer = producer or self._producer

        notification = {}
        notification['topic'] = self._topic

        # Add headers
        headers = {
                   'type': 'cms_wmagent_info',
                   'version': self._version,
                   'producer': producer
        }

        notification.update(headers)

        # Add body consisting of the payload and metadata
        body = {
            'payload': payload,
            'metadata': {
                'timestamp': int(time.time()),
                'id': id_,
                'uuid': str(uuid.uuid1()),
            }
        }

        notification['body'] = body

        return notification
    
