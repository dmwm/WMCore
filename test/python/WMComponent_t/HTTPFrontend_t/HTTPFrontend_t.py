from MessageService.MessageService import MessageService
from WMComponent_t.HTTPFrontend_t.MsgServiceApp import MsgServiceApp
from WMCore.Database.Transaction import Transaction
import threading

msApp = MsgServiceApp("HTTPFrontend_t")
ms = msApp.msgService()
print "PUBLISH"
myThread = threading.currentThread()
myThread.transaction = Transaction(myThread.dbi)
myThread.transaction.begin()
ms.subscribeTo("HTTPFrontendStart")

ms.purgeMessages()
ms.publish({'name' : 'HTTPFrontendStart', 'payload' : 'GO', 'delay' : '00:00:00', 'instant' : True})
myThread.transaction.commit()
ms.finish()

myThread.transaction.begin()
print str(ms.pendingMsgs())
myThread.transaction.commit()


print ms.get(False)

