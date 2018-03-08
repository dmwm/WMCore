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
    # fraction of condor schedd limit, used for job submission
    "CondorJobsFraction": 0.75,
    # In JobSumitter, submit jobs over the threshold if the priority is higher than current pending/running jobs
    "CondorOverflowFraction": 0.2,  # default 20% over threshold
    # ExitCodes returned by jobs which doesn't need to be retried
    "NoRetryExitCodes": [70, 73, 8001, 8006, 8009, 8023, 8026, 8501, 50660, 50661, 50664, 71102],
    # Number of times a job will be retried
    "MaxRetries" : {'default': 3, 'Merge': 4, 'Cleanup': 2, 'LogCollect': 1, 'Harvesting': 2}
}
