import signal
import logging
import time

from threading import Thread
from pprint import pformat


def _countdown(endTime):
    """
    _countdown_

    Aux function to return the remaining time before reaching the endTime
    :param endTime: The end time in seconds since the epoch.
    :return:        Remaining time in seconds or 0 if the endTime has already passed
                    No negative values are returned
    """
    remTime = endTime - time.time()
    if remTime < 0:
        remTime = 0
    return remTime


class Timer(Thread):
    """
    Watchdog timer class.
    The instances  of this class are about to a separate thread and are to
    expect one particular PID to reset the timer. If the timer reaches to the end
    of its interval, without being reset, it would restart the component it is associated with.
    """
    def __init__(self, name=None, compName=None, expPids=[], expSig=None, interval=0, config=None):
        """
        __init__
        :param expPids:  The list of expected pids allowed to reset the timer, signals from anybody else would be ignored
        :param compName: The component name this timer is associated with
        :param expSig:   The default signal to be expected for resetting the timer (Default: SIGCONT)
        :param interval: The interval for the timer
        """
        Thread.__init__(self)
        self.name = name
        self.compName = compName
        self.interval = interval
        self.config = config
        self.expPids = expPids
        self.expSig = expSig or signal.SIGCONT
        self.daemon = True


    def run(self):
        """
        _run_
        Thread class run() method override
        """
        self._timer()

    def _timer(self):
        """
        A simple timer method, which will be used to override the main Thread class run() method.
        It would expect one particular PID to reset itself. If the timer reaches to the end of
        its interval, without being reset, it would restart the component it is associated with.
        :return:         Nothing
        """

        startTime = time.time()
        endTime = startTime + self.interval

        while True:
            sigInfo = signal.sigtimedwait([self.expSig], _countdown(endTime))
            if sigInfo:
                logging.info(f"Timer: {self.name}, pid: {self.native_id} : Received signal: {pformat(sigInfo)}")
                if sigInfo.si_pid in self.expPids:
                    # Resetting the timer starting again from the current time
                    logging.info(f"Timer: {self.name}, pid: {self.native_id} : Resetting timer")
                    endTime = time.time() + self.interval
                else:
                    # Continue to wait for signal from the correct origin
                    logging.info(f"Timer: {self.name}, pid: {self.native_id} : Continue to wait for signal from the correct origin. Remaining time: {_countdown(endTime)}")
                    continue
            else:
                logging.info(f"Timer: {self.name}, pid: {self.native_id} : Reached the end of timer.")
                break
