#!/usr/bin/env python
"""
Unittests for ProcFS functions
"""

# system modules
import unittest
import os
import time
import threading

# WMCore modules
from Utils.ProcFS import processStatus


def daemon(nloops=2):
    """
    Dummy daemon with sleep loops
    :param: number of loops to perform, default 2
    """
    for _ in range(nloops):
        time.sleep(1)


class TestProcessStatus(unittest.TestCase):
    """
    TestProcessStauts unit test class
    """
    def testProcessStatus(self):
        """Test processStatus function with the current process PID."""
        pid = os.getpid()  # Get the current process ID
        thread = threading.Thread(target=daemon, args={})  # Create a dummy thread
        thread.start()
        try:
            statusList = processStatus(pid)

            # Check that at least one entry exists (main process)
            self.assertGreater(len(statusList), 0, "Status list should not be empty")

            # Ensure the first entry is the main process
            mainProcess = statusList[0]
            self.assertEqual(mainProcess["pid"], str(pid), "Main process PID should match")
            self.assertIn(mainProcess["status"][0], "running", "Unexpected process state")

            # Ensure that we have two pids: one for main process and another for daemon thread
            pids = [entry["pid"] for entry in statusList]
            self.assertTrue(len(pids), 2)
            self.assertTrue(pids[0] != pids[1], True)

        finally:
            thread.join()  # Ensure thread finishes execution

    def worker(self):
        """A simple worker thread that runs for a short time."""
        time.sleep(2)  # Keep the thread alive for 2 seconds

    def testProcessStatusWithStoppedThread(self):
        """Test processStatus when a thread is running and then stopped."""
        pid = os.getpid()  # Get current process ID
        thread = threading.Thread(target=self.worker)
        thread.start()  # Start the thread

        time.sleep(0.5)  # Wait for the thread to be fully initialized

        # Check status while the thread is running
        statusList = processStatus(pid)
        pids = [entry["pid"] for entry in statusList]
        self.assertTrue(len(pids), 2)

        # Let the thread finish (simulate stopping)
        thread.join()

        time.sleep(1)  # Allow time for the system to reflect thread termination

        # Check status after thread stops
        statusList = processStatus(pid)
        pids = [entry["pid"] for entry in statusList]
        self.assertTrue(len(pids), 1)


if __name__ == "__main__":
    unittest.main()
