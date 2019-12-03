#!/usr/bin/env python
"""
Use the builtin WorkQueue fixConflicts method to fix conflicts
in the local workqueue and workqueue_inbox.

You need to source the agent libraries with:
source apps/wmagent/etc/profile.d/init.sh

then it's suggested to executed it as:
python fixConflicts.py | tee conflicts_fixed.log
"""
from __future__ import print_function

import os
import sys
try:
    from WMCore.Configuration import loadConfigurationFile
    from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
except ImportError as e:
    print("You do not have a proper environment (%s), please source the following:" % str(e))
    print("source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh")
    sys.exit(1)


def main():

    os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    # fix conflicts found in the local queue
    backend = WorkQueueBackend(config.WorkQueueManager.couchurl)
    backend.fixConflicts()

if __name__ == '__main__':
    sys.exit(main())
