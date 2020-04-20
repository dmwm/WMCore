from __future__ import division, print_function

DEFAULT_AGENT_CONFIG = {
    # user drain mode which is decided by user
    "UserDrainMode": False,
    # agent drain mode automatically calculated by agent
    "AgentDrainMode": False,
    # speed drain flag used for informational purposes only (reported back to wmstats)
    "SpeedDrainMode": False,
    # disk usage percentage for warning
    "DiskUseThreshold": 85,
     # list of disks which shouldn't be included in monitoring for the Threshold
    "IgnoreDisks": ["/mnt/ramdisk"],
    # fraction of condor schedd limit, used for job submission
    "CondorJobsFraction": 0.75,
    # In JobSumitter, submit jobs over the threshold if the priority is higher than current pending/running jobs
    "CondorOverflowFraction": 0.2,  # default 20% over threshold
    # ExitCodes returned by jobs which doesn't need to be retried
    "NoRetryExitCodes": [70, 73, 8001, 8006, 8009, 8023, 8026, 8501, 50660, 50661, 50664, 71102, 71104, 71105],
    # Number of times a job will be retried
    "MaxRetries" : {'default': 3, 'Merge': 4, 'Cleanup': 2, 'LogCollect': 1, 'Harvesting': 4},
    # Speed drain thresholds - a draining agent's config is updated when # of remaining condor jobs is below a threshold
    # CondorPriority: increase condor priority for all queued Production/Processing jobs to 999999
    # NoJobRetries: change MaxRetries to 0, all job errors are terminal
    # EnableAllSites: submit jobs to any site
    "SpeedDrainConfig": {'CondorPriority': {'Threshold': 500, 'Enabled': False},
                         'NoJobRetries':   {'Threshold': 200, 'Enabled': False},
                         'EnableAllSites': {'Threshold': 200, 'Enabled': False}
                        }
}
