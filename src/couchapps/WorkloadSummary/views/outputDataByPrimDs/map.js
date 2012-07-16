/**
 * This view gets the summaries with an Id that matches any the following patterns:
 * Run<runnumber>-<PrimDs>-<ProcessedDataset>-Tier1PromptReco-<GUID>
 * Run<runnumber>-<PrimDs>-<ProcessedDataset>-<SkimName>-<GUID>
 * It returns them with the following key:
 * [<PrimDs>,<isTier1Reco>, <runnumber>]
 *
 */
function(doc) {
    var regexp = /^Run(\d+)-(\w+)-(\w+)(-\w+)?-v(\d+)-(\w+)/;
    var match = doc._id.match(regexp);
    if (match && match.length == 7){
       var runNumber = parseInt(match[1]);
       var primDs = match[2];
       var isReco = (match[4] != null) ? 0 : 1;
       emit([primDs, isReco, runNumber], null);
    }
}

