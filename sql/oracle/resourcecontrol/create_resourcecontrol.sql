-- Oracle tables for WMCore.ResourceControl component

CREATE TABLE rc_threshold (
    site_id       NUMBER(11)    NOT NULL,
    sub_type_id   NUMBER(11)    NOT NULL,
    pending_slots NUMBER(11)    NOT NULL,
    max_slots     NUMBER(11)    NOT NULL,
    CONSTRAINT rc_threshold_fk1 FOREIGN KEY (site_id) 
        REFERENCES wmbs_location(id) ON DELETE CASCADE,
    CONSTRAINT rc_threshold_fk2 FOREIGN KEY (sub_type_id) 
        REFERENCES wmbs_sub_types(id) ON DELETE CASCADE
)
/ 