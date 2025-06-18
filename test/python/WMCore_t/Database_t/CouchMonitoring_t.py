"""
CouchMonitoring_t module provide unit tests for CouchMonitoring module
"""

import unittest
from unittest.mock import patch, MagicMock, mock_open
from WMCore.Database.CouchMonitoring import (
    getSchedulerJobDocs, getReplicatorDocs, compareReplicationStatus,
    formatPrometheusMetrics, createAlerts, checkStatus
)

class TestCouchReplicationModule(unittest.TestCase):
    """
    TestCouchReplicationModule provides unit tests for CouchMonitoring module
    """

    @patch('WMCore.Database.CouchMonitoring.requests.get')
    @patch('WMCore.Database.CouchMonitoring.couchCredentials', return_value=('user', 'pass'))
    def testGetSchedulerJobDocsSuccess(self, mockCreds, mockGet):
        """
        Mockered unit test for scheduler jobs monitoring
        """
        mockGet.return_value.json.return_value = {
            'jobs': [
                {
                    'doc_id': 'doc1',
                    'source': 'db1',
                    'target': 'db2',
                    'history': [{'type': 'started'}],
                    'info': {}
                }
            ]
        }
        mockGet.return_value.raise_for_status = MagicMock()

        result = getSchedulerJobDocs("http://localhost:5984")
        self.assertIn('doc1', result)
        self.assertEqual(result['doc1']['state'], 'started')

    @patch('WMCore.Database.CouchMonitoring.requests.get')
    @patch('WMCore.Database.CouchMonitoring.couchCredentials', return_value=('user', 'pass'))
    def testGetReplicatorDocsSuccess(self, mockCreds, mockGet):
        """
        Mockered unit test for replicator monitoring
        """
        mockGet.return_value.status_code = 200
        mockGet.return_value.json.return_value = {
            'rows': [
                {'doc': {
                    '_id': 'rep1',
                    '_replication_state': 'completed',
                    'source': 'db1',
                    'target': 'db2',
                    '_replication_history': [
                        {'start_time': '2024-01-01T00:00:00Z', 'type': 'completed'}
                    ]
                }}
            ]
        }

        result = getReplicatorDocs("http://localhost:5984")
        self.assertIn('rep1', result)
        self.assertEqual(result['rep1']['state'], 'completed')

    def testCompareReplicationStatusDetectsChanges(self):
        """
        unit test for detecting changes in monitoring documents
        """
        prev = {'doc1': {'state': 'started'}}
        curr = {'doc1': {'state': 'completed'}}
        changes = compareReplicationStatus(prev, curr)
        self.assertIn('doc1', changes)
        self.assertEqual(changes['doc1']['old'], {'state': 'started'})
        self.assertEqual(changes['doc1']['new'], {'state': 'completed'})

    def testFormatPrometheusMetricsGeneratesOutput(self):
        """
        unit test for prometheus metrics
        """
        statuses = {
            'rep1': {
                'state': 'error',
                'source': 'src',
                'target': 'tgt',
                'error': None,
                'history': []
            }
        }
        output = formatPrometheusMetrics(statuses)
        self.assertIn('couchdb_replication_state{replId="rep1"', output)
        self.assertIn('-1', output)

    def testCreateAlertsDetectsIssues(self):
        """
        unit test for alert metrics
        """
        statuses = {
            'rep1': {
                'state': 'error',
                'source': 'src',
                'target': 'tgt',
                'error': 'crashed',
                'history': []
            },
            'rep2': {
                'state': 'completed',
                'source': 'src',
                'target': 'tgt',
                'error': None,
                'history': []
            }
        }
        alerts = createAlerts(statuses)
        self.assertIn('rep1', alerts)
        self.assertNotIn('rep2', alerts)

    @patch('WMCore.Database.CouchMonitoring.getSchedulerJobDocs')
    @patch('WMCore.Database.CouchMonitoring.formatPrometheusMetrics')
    @patch('WMCore.Database.CouchMonitoring.createAlerts')
    def testCheckStatusSchedulerKind(self, mockAlerts, mockMetrics, mockScheduler):
        """
        unit test for checking status scheduler metrics
        """
        mockScheduler.return_value = {
            'job1': {
                'state': 'started',
                'source': 's1',
                'target': 't1',
                'error': None,
                'history': []
            }
        }
        mockMetrics.return_value = 'mock_metrics'
        mockAlerts.return_value = {}

        result = checkStatus(url="http://localhost:5984", kind='scheduler')
        self.assertIn('current_status', result)
        self.assertEqual(result['metrics'], 'mock_metrics')

    @patch('WMCore.Database.CouchMonitoring.getReplicatorDocs')
    @patch('WMCore.Database.CouchMonitoring.formatPrometheusMetrics')
    @patch('WMCore.Database.CouchMonitoring.createAlerts')
    def testCheckStatusReplicatorKind(self, mockAlerts, mockMetrics, mockReplicator):
        """
        unit test for checking status replicator metrics
        """
        mockReplicator.return_value = {
            'job1': {
                'state': 'completed',
                'source': 's1',
                'target': 't1',
                'error': None,
                'history': []
            }
        }
        mockMetrics.return_value = 'mock_metrics'
        mockAlerts.return_value = {}

        result = checkStatus(url="http://localhost:5984", kind='replicator')
        self.assertIn('current_status', result)
        self.assertEqual(result['metrics'], 'mock_metrics')

    def testCompareReplicationStatusNoChanges(self):
        """
        unit test for checking status metrics with no changes
        """
        status = {'doc1': {'state': 'started'}}
        changes = compareReplicationStatus(status, status.copy())
        self.assertEqual(changes, {})


if __name__ == '__main__':
    unittest.main()
