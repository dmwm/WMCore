import time
import unittest
import smtplib

try:
    import mox
except ImportError:
    raise ImportError("Python MOX must be installed to run this unittest. "
                      "http://code.google.com/p/pymox/")

from WMCore.Configuration import ConfigSection
from WMCore.Alerts.Alert import Alert
import WMCore.Alerts.ZMQ.Sinks.EmailSink as EmailSinkMod
from WMCore.Alerts.ZMQ.Sinks.EmailSink import EmailSink



class EmailSinkTest(unittest.TestCase):
    def setUp(self):
        self.config = ConfigSection("email")
        self.config.fromAddr = "some@local.com"
        self.config.toAddr = ["some1@local.com", "some2@local.com"]
        self.config.smtpServer = "smtp.gov"
        self.config.smtpUser = None
        self.config.smtpPass = None

        # now we want to mock smtp emailing stuff - via pymox - no actual
        # email sending to happen
        self.mox = mox.Mox()
        self.smtpReal = EmailSinkMod.smtplib
        EmailSinkMod.smtplib = self.mox.CreateMock(EmailSinkMod.smtplib)
        self.smtp = self.mox.CreateMockAnything()


    def tearDown(self):
        self.mox.UnsetStubs()
        EmailSinkMod.smtplib = self.smtpReal


    def testEmailSinkBasic(self):
        # pre-generate the entire email message
        subj = "Alert from %s" % None # this is default Alert value for HostName
        msg = EmailSink.EMAIL_HEADER % (self.config.fromAddr, subj,
                                        ", ".join(self.config.toAddr))
        alerts = []
        for i in range(10):
            a = Alert(Source=__file__, Level = i, Timestamp = time.time(), Type = "Test")
            msg += "\n%s\n" % a.toMsg()
            alerts.append(a)

        # method calls definition, ordered
        EmailSinkMod.smtplib.SMTP(self.config.smtpServer).AndReturn(self.smtp) # 1
        # leave for test / debugging
        # self.smtp.sendmail('a@b.com', 'a@b.com', 'Subject: subject\n\nbody')
        self.smtp.sendmail(self.config.fromAddr, self.config.toAddr, msg) # 2
        self.smtp.quit() # 3

        self.mox.ReplayAll()

        sink = EmailSink(self.config) # 1
        # leave for test / debugging
        #self.smtp.sendmail('a@b.com', 'a@b.com', 'Subject: subject\n\nbody')
        sink.send(alerts) # 2
        del sink # 3

        self.mox.VerifyAll()



if __name__ == "__main__":
    unittest.main()
