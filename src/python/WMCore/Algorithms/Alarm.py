#!/usr/bin/env python

"""
_Alarm_

Silly, pointless, bogus classes that you have to have
in order to raise an alarm

Mainly, this is just where I stick the documentation:

To run this you need to use python's signal module:

First set the alarm:
signal.signal(signal.SIGALRM, alarmHandler)
signal.alarm(waitTime)

Then run your code:
try:
  doX()
except Alarm:
  pass

Afterwards, reset the alarm

signal.alarm(0)
"""

class Alarm(Exception):
    """
    Silly exception

    """
    pass

def alarmHandler(signum, frame):
    """
    Silly handler (raise if you have an alarm)
    """
    raise Alarm
