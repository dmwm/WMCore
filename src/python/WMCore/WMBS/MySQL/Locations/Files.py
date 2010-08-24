sql = """select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_file_location where location =
                    (select id from wmbs_location where se_name = :location))
        """