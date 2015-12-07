/*
 Modified Based on the YUI code
 Copyright (c) 2009, Yahoo! Inc. All rights reserved.
 Code licensed under the BSD License:
 http://developer.yahoo.net/yui/license.txt
 version: 2.8.0r4
 */
if (typeof WMStats == "undefined" || !WMStats) {
    var WMStats = {};
}

WMStats.namespace = function(){
    var A = arguments, E = null, C, B, D;
    for (C = 0; C < A.length; C = C + 1) {
        D = ("" + A[C]).split(".");
        E = WMStats;
        for (B = (D[0] == "WMStats") ? 1 : 0; B < D.length; B = B + 1) {
            E[D[B]] = E[D[B]] || {};
            E = E[D[B]];
        }
    }
    return E;
};
