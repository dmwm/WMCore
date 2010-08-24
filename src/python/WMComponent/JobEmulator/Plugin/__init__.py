
"""

There are several types of plugins used in the job emulator:

Completion : how and what percentage of jobs terminate successful.
Report: specifies how reports (success and failure) are generated
Scheduler: simulates mapping from a job to an actual node
Submitter: plugins to simulate job submission. These plugins can be used
in the jobemulator but instead also inthe submitter component.
Tracker: Used to track simulated jobs. This plugin can be used in the jobemulator
and then uses a socalled placebo tracker db or it can be plugged into a real
tracker component and acesses a genuine tracker database

"""

