#!/usr/bin/env python

from __future__ import (division, print_function)

endpoint = 'https://cmsweb-prod.cern.ch/dbs/prod/phys03/DBSReader'

datasets = []

calls = [
    ['listBlockOrigin', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caabcd'}],
    ['listBlockOrigin', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace'}],
    ['listBlockOrigin', {'block_name': '/HighPileUp/Run2011A-v1/RAW#6021175e-cbfb-11e0-80a9-003048caaace'}],
    ['listBlockOrigin', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caabcdabcd'}],
    ['listBlockOrigin', {
        'block_name': '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-ea0972193530f531086947d06eb0f121/USER#fb978442-a61b-413a-b4f4-526e6cdb142e'}],
    ['listBlockOrigin', {
        'block_name': '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-ea0972193530f531086947d06eb0f121/USER#0b04d417-d734-4ef2-88b0-392c48254dab'}],
]
