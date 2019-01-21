#!/usr/bin/env python
"""
Basic interface to CERN ActiveMQ via stomp
"""
from __future__ import print_function
from __future__ import division

import json
import logging
import time
import stomp
from WMCore.Services.UUIDLib import makeUUID


class StompyListener(object):
    """
    Auxiliar listener class to fetch all possible states in the Stomp
    connection.
    """
    def __init__(self, logger=None):
        self.logger = logger if logger else logging.getLogger()

    def on_connecting(self, host_and_port):
        self.logger.debug('on_connecting %s', str(host_and_port))

    def on_error(self, headers, message):
        self.logger.debug('received an error %s %s', str(headers), str(message))

    def on_message(self, headers, body):
        self.logger.debug('on_message %s %s', str(headers), str(body))

    def on_heartbeat(self):
        self.logger.debug('on_heartbeat')

    def on_send(self, frame):
        self.logger.debug('on_send HEADERS: %s, BODY: %s ...', str(frame.headers), str(frame.body)[:160])

    def on_connected(self, headers, body):
        self.logger.debug('on_connected %s %s', str(headers), str(body))

    def on_disconnected(self):
        self.logger.debug('on_disconnected')

    def on_heartbeat_timeout(self):
        self.logger.debug('on_heartbeat_timeout')

    def on_before_message(self, headers, body):
        self.logger.debug('on_before_message %s %s', str(headers), str(body))

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
    :param cert: path to certificate file
    :param key: path to key file
    """

    # Version number to be added in header
    _version = '0.3'

    def __init__(self, username, password, producer, topic,
                 host_and_ports=None, logger=None, cert=None, key=None):
        self._username = username
        self._password = password
        self._producer = producer
        self._topic = topic
        self._host_and_ports = host_and_ports or [('agileinf-mb.cern.ch', 61213)]
        self.logger = logger if logger else logging.getLogger()
        self._cert = cert
        self._key = key
        self._use_ssl = True if key and cert else False
        # silence the INFO log records from the stomp library, until this issue gets fixed:
        # https://github.com/jasonrbriggs/stomp.py/issues/226
        logging.getLogger("stomp.py").setLevel(logging.WARNING)

    def send(self, data):
        """
        Connect to the stomp host and send a single notification
        (or a list of notifications).

        :param data: Either a single notification (as returned by
            `make_notification`) or a list of such.

        :return: a list of notification bodies that failed to send
        """
        # If only a single notification, put it in a list
        if isinstance(data, dict) and 'body' in data:
            data = [data]

        conn = stomp.Connection(host_and_ports=self._host_and_ports)

        if self._use_ssl:
            # This requires stomp >= 4.1.15
            conn.set_ssl(for_hosts=self._host_and_ports, key_file=self._key, cert_file=self._cert)

        conn.set_listener('StompyListener', StompyListener(self.logger))
        try:
            conn.start()
            # If cert/key are used, ignore username and password
            if self._use_ssl:
                conn.connect(wait=True)
            else:
                conn.connect(username=self._username, passcode=self._password, wait=True)

        except stomp.exception.ConnectFailedException as exc:
            self.logger.error("Connection to %s failed %s", repr(self._host_and_ports), str(exc))
            return []

        failedNotifications = []
        for notification in data:
            result = self._send_single(conn, notification)
            if result:
                failedNotifications.append(result)

        if conn.is_connected():
            conn.disconnect()

        if failedNotifications:
            self.logger.warning('Failed to send to %s %i docs out of %i', repr(self._host_and_ports),
                                len(failedNotifications), len(data))

        return failedNotifications

    def _send_single(self, conn, notification):
        """
        Send a single notification to `conn`

        :param conn: An already connected stomp.Connection
        :param notification: A dictionary as returned by `make_notification`

        :return: The notification body in case of failure, or else None
        """
        try:
            body = notification.pop('body')
            conn.send(destination=self._topic,
                      headers=notification,
                      body=json.dumps(body),
                      ack='auto')
            self.logger.debug('Notification %s sent', str(notification))
        except Exception as exc:
            self.logger.error('Notification: %s not send, error: %s', str(notification), str(exc))
            return body
        return

    def make_notification(self, payload, docType, docId=None, producer=None, ts=None, metadata=None,
                          dataSubfield="data"):
        """
        Produce a notification from a single payload, adding the necessary
        headers and metadata. Generic metadata is generated to include a
        timestamp, producer name, document id, and a unique id. User can
        pass additional metadata which updates the generic metadata.

        If payload already contains a metadata field, it is overwritten.

        :param payload: Actual data.
        :param docType: document type for metadata.
        :param docId: document id representing the notification. If none provided,
               a unique id is created.
        :param producer: The notification producer name, taken from the StompAMQ
               instance producer name by default.
        :param ts: timestamp to be added to metadata. Set as time.time() by default
        :param metadata: dictionary of user metadata to be added. (Updates generic
               metadata.)
        :param dataSubfield: field name to use for the actual data. If none, the data
               is put directly in the body. Default is "data"

        :return: a single notifications with the proper headers and metadata
        """
        producer = producer or self._producer
        umetadata = metadata or {}
        ts = ts or int(time.time())
        uuid = makeUUID()
        docId = docId or uuid

        headers = {'type': docType,
                   'version': self._version,
                   'producer': producer}

        metadata = {'timestamp': ts,
                    'producer': producer,
                    '_id': docId,
                    'uuid': uuid}
        metadata.update(umetadata)

        body = {}
        if dataSubfield:
            body[dataSubfield] = payload
        else:
            body.update(payload)
        body['metadata'] = metadata

        notification = {}
        notification.update(headers)
        notification['body'] = body

        return notification
