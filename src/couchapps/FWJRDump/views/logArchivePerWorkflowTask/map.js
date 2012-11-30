function (doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    task = doc.fwjr.task.split('/');
    taskType = task.slice(-1)[0];
    isLogCollect = taskType.indexOf('LogCollect');
    if (isLogCollect >= 0) {
        if (null != doc.fwjr.steps.logCollect1) {
            logCollectStep = doc.fwjr.steps.logCollect1;
            logArchivePFN = logCollectStep.output.LogCollect[0].lfn;
            emit(doc.fwjr.task, logArchivePFN);
        }
    }
  }
}
