/**
 * This view gets the summaries with an Id that matches any the following patterns:
 * Run<runnumber>-<PrimDs>-<ProcessedDataset>-Tier1PromptReco-<GUID>
 * Run<runnumber>-<PrimDs>-<ProcessedDataset>-<SkimName>-<GUID>
 * It returns them with the following key:
 * [<PrimDs>,<WorkflowType>, <runnumber>]
 * The possible workflow types are:
 *    T1Reco for the first pattern
 *    PromptSkim for the second pattern
 */
function(doc) {
    var regexp = /^Run(\d+)-(\w+)-(\w+)(-\w+)?-v(\d+)-(\w+)/;
    var match = doc._id.match(regexp);
    if (match && match.length == 7){
       var runNumber = parseInt(match[1]);
       var primDs = match[2];
       var isReco = (match[4] != null) ? "PromptSkim" : "T1Reco";
       emit([primDs, isReco, runNumber], null);
    }
}

