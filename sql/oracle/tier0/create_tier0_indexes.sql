-- Index creation for Tier0 Oracle schema

-- Run table indexes
CREATE INDEX run_status_idx ON run(status);
CREATE INDEX run_express_released_idx ON run(express_released);
CREATE INDEX run_in_datasvc_idx ON run(in_datasvc);
CREATE INDEX run_start_time_idx ON run(start_time);
CREATE INDEX run_stop_time_idx ON run(stop_time);
CREATE INDEX run_close_time_idx ON run(close_time);

-- Association table indexes
CREATE INDEX run_trig_primds_assoc_primds_idx ON run_trig_primds_assoc(primds_id);
CREATE INDEX run_trig_primds_assoc_trig_idx ON run_trig_primds_assoc(trig_id);

CREATE INDEX run_primds_stream_assoc_stream_idx ON run_primds_stream_assoc(stream_id);

CREATE INDEX run_primds_scenario_assoc_scenario_idx ON run_primds_scenario_assoc(scenario_id);

CREATE INDEX run_stream_style_assoc_style_idx ON run_stream_style_assoc(style_id);

CREATE INDEX run_stream_cmssw_assoc_cmssw_idx ON run_stream_cmssw_assoc(online_version);

CREATE INDEX run_stream_done_in_datasvc_idx ON run_stream_done(in_datasvc);

-- Lumi section indexes
CREATE INDEX lumi_section_closed_stream_idx ON lumi_section_closed(stream_id);
CREATE INDEX lumi_section_closed_lumi_idx ON lumi_section_closed(lumi_id);
CREATE INDEX lumi_section_closed_close_time_idx ON lumi_section_closed(close_time);

CREATE INDEX lumi_section_split_active_run_idx ON lumi_section_split_active(run_id);
CREATE INDEX lumi_section_split_active_lumi_idx ON lumi_section_split_active(lumi_id);

-- Streamer indexes
CREATE INDEX streamer_p5_id_idx ON streamer(p5_id);
CREATE INDEX streamer_run_id_idx ON streamer(run_id);
CREATE INDEX streamer_stream_id_idx ON streamer(stream_id);
CREATE INDEX streamer_lumi_id_idx ON streamer(lumi_id);
CREATE INDEX streamer_used_idx ON streamer(used);
CREATE INDEX streamer_deleted_idx ON streamer(deleted);
CREATE INDEX streamer_skipped_idx ON streamer(skipped);

-- Configuration table indexes
CREATE INDEX reco_release_config_in_datasvc_idx ON reco_release_config(in_datasvc);
CREATE INDEX reco_release_config_released_idx ON reco_release_config(released);

CREATE INDEX express_config_in_datasvc_idx ON express_config(in_datasvc);

CREATE INDEX reco_config_in_datasvc_idx ON reco_config(in_datasvc);
CREATE INDEX reco_config_do_reco_idx ON reco_config(do_reco);

-- Monitoring table indexes
CREATE INDEX workflow_monitoring_tracked_idx ON workflow_monitoring(tracked);
CREATE INDEX workflow_monitoring_closeout_idx ON workflow_monitoring(closeout); 