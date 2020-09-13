"""
It defines and provides some data structures required by the
Transferor and Monitor threads.
It isn't yet enforced, if any new keys are needed, it's better
to get them added here as well.
"""
from __future__ import division, print_function

# summary metrics for the MSMonitor thread
# (also available through the `info` REST API)
MONITOR_REPORT = dict(start_time=0,
                      end_time=0,
                      execution_time=0,
                      error="",
                      total_num_campaigns=0,
                      total_num_transfers=0,
                      filtered_transfer_docs=0,
                      success_transfer_doc_update=0,
                      failed_transfer_doc_update=0,
                      request_status_updated=0)

# summary metrics for the MSTransferor thread
# (also available through the `info` REST API)
TRANSFEROR_REPORT = dict(start_time=0,
                         end_time=0,
                         execution_time=0,
                         error="",
                         total_num_requests=0,
                         total_num_campaigns=0,
                         nodes_out_of_space=None,
                         success_request_transition=0,
                         failed_request_transition=0,
                         problematic_requests=0,
                         num_datasets_subscribed=0,
                         num_blocks_subscribed=0)

# structure for a transfer document to be written to central CouchDB
TRANSFER_COUCH_DOC = dict(workflowName="",
                          lastUpdate=0,
                          transfers=None)

# structure of a transfer record, it corresponds to the `transfers` field in
# the TRANSFER_COUCH_DOC structure
TRANSFER_RECORD = dict(dataset="",
                       dataType="",
                       transferIDs=set(),
                       campaignName="",
                       completion=[0.0])

# summary metrics for the MSOutput thread
# (also available through the `info` REST API)
OUTPUT_REPORT = dict(thread_id="",
                     start_time=0,
                     end_time=0,
                     execution_time=0,
                     error="",
                     total_num_requests=0,
                     total_num_campaigns=0,
                     num_datasets_subscribed=0,
                     num_data_requests=0)
