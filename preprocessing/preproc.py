import logging
import os
from preprocessing import strip_text_from_raw_dump, wiki_dump_to_dataframe
import datetime
import pandas

# command to run:
# python3 -m preprocessing.preproc <path_to_input_file> <path_to_output_dir> INFO

if __name__ == "__main__":
    import sys

    #TODO add audit data- nmber of records, number of pages, revisions
    # time taken to preprocess
    # input data size, output data size
    #


    if len(sys.argv) < 4:
        logging.error("usage: preproc.py <IN_FILE/IN_DIR> <OUT_DIR> <logging_level>")
    input_path = sys.argv[1]
    out_dir = sys.argv[2]
    loglevel = sys.argv[3]

    if os.path.isfile(input_path):
        input_mode = 'single_file'
        in_filename = os.path.basename(input_path)
    else:
        input_mode = 'split_file'
        in_dir = input_path



    log_dir = os.path.join(out_dir, "logs")
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    start_time = datetime.datetime.now()
    log_filename = start_time.isoformat().replace(":", "-").replace(".", "-") + os.path.basename(input_path) + "log.txt"

    # set logging config
    # assuming loglevel is bound to the string value obtained from the
    # command line argument. Convert to upper case to allow the user to
    # specify --log=DEBUG or --log=debug
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(filename = os.path.join(log_dir, log_filename), level=numeric_level)

    def preprocess_file(in_file_path):
        in_filename = os.path.basename(in_file_path)
        logging.info("removing text data from wiki dump: " + in_filename)
        dump_rmtext_file = os.path.join(out_dir, "rmtext_" + in_filename)
        strip_text_from_raw_dump.strip_text(in_file_path, dump_rmtext_file)
        logging.info("dump less text data saved at: " + dump_rmtext_file)
        rmtext_endtime = datetime.datetime.now()
        logging.info("time taken for rmtext " + str(rmtext_endtime - start_time) )

        logging.info("extracting fields from xml dump to csv format")

        revisions_df = wiki_dump_to_dataframe.extract_fields_from_xml(dump_rmtext_file)
        to_dataframe_endtime = datetime.datetime.now()
        logging.info("time taken for transforming to csv " + str(to_dataframe_endtime - rmtext_endtime))
        return revisions_df

    if input_mode == 'single_file':
        in_filename = os.path.basename(input_path)
        revisions_df = preprocess_file(input_path)
        dump_csv_file = os.path.join(out_dir, "wiki_fields_" + in_filename + ".csv")
        wiki_dump_to_dataframe.revisions_df_to_csv(revisions_df, dump_csv_file)
        logging.info("wiki fields csv saved at: " + dump_csv_file)

    else:
        in_file_list = os.listdir(in_dir)[1:]
        revisions_combine = pandas.DataFrame()
        dir_name = os.path.basename(in_dir)
        dump_csv_file = os.path.join(out_dir, "wiki_fields_" + dir_name + ".csv")
        for in_file in in_file_list:
            revisions_df = preprocess_file(os.path.join(in_dir, in_file))
            print("revisions_df size: " + str(len(revisions_df)))
            print("preprocessing completed for" + in_file )
            revisions_combine = pandas.concat([revisions_combine, revisions_df])
            print("revisions_combine size: " + str(len(revisions_combine)))
        wiki_dump_to_dataframe.revisions_df_to_csv(revisions_combine, dump_csv_file)
        logging.info("wiki fields csv saved at: " + dump_csv_file)

