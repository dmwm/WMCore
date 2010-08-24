#!/usr/bin/env python
"""
_TaskQueueFixedMonitor_

Returns always a fixed value (in theory one can always enqueue new tasks
into the TaskQueue).

WARNING: The TaskQueue is "human", so we should require an absolute maximum!
Use TaskQueueThresholdMonitor instead.

"""
from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor


# Fixed value of tasks to enqueue at each iteration
# TODO: Should come from the cfg...
_FixedValue = 25

class TaskQueueFixedMonitor(MonitorInterface):
    """
    _TaskQueueFixedMonitor_

    Returns always a fixed value (in theory one can always enqueue new tasks
    into the TaskQueue).
    """

    def __call__(self):
        """
        _operator()_
        """
        constraint = self.newConstraint()
        constraint['count'] = _FixedValue
        return [constraint]


registerMonitor(TaskQueueFixedMonitor, TaskQueueFixedMonitor.__name__)

