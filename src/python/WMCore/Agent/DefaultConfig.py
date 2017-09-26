from __future__ import division, print_function

DEFAULT_AGENT_CONFIG = {
    # user drain mode which is decided by user
    "UserDrainMode": False,
     # agent drain mode automatically calculated by agent
    "AgentDrainMode": False,
     # disk usage percentage for warning
    "DiskUseThreshold": 85,
     # list of disks which shouldn't be included in monitoring for the Threshold
    "IgnoreDisks": ["/lustre/unmerged"],
    # ExitCodes returned by jobs which doesn't need to be retried
    "NoRetryExitCodes": [70, 73, 8001, 8006, 8009, 8023, 8026, 50660, 50661, 50664, 71102]
}