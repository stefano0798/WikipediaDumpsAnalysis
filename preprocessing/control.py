import subprocess
import sys
import datetime
import os
import logging
from preprocessing import download_dump_files
from preprocessing import strip_text_and_extract_fields_pagelevel
# from preprocessing import wiki_dump_to_dataframe_lxml

# 1. download compressed file
# 2. uncompress
# 3. remove text data
# 4. read xml - transform to dataframe
# 5. save as csv file

DOWNLOAD_URL_LIST_FILE = '/home/s2575760/project/wiki_bigdata/resources/wikidumps_source_download_list.txt'
DOWNLOAD_FILES_LOCATION = '/home/s2575760/project/wiki_dump_files/full_page_history_dumps/post500'
FILES_UNCOMPRESS_LOCATION = '/home/s2575760/project/wiki_dump_files/preproc_inter/unzipped'
FILES_SPLIT_LOCATION = '/home/s2575760/project/wiki_dump_files/preproc_inter/split' # NOT USED
FILES_RMTEXT_LOCATION = '/home/s2575760/project/wiki_dump_files/preproc_inter/rmtext' # NOT USED
FILES_CSV_LOCATION = '/home/s2575760/project/wiki_preprocessed'

DOWNLOAD_FILES_HDFS_LOCATION = '/user/s2575760/project/data/enwiki_compressed'
PREPROCESSED_FILES_HDFS_LOCATION = '/user/s2575760/project/data/enwiki-202012010-pages-meta-history'
APPLICATION_LOGS_LOCATION = '/home/s2575760/project/logs/'

def unzip_file(file_path, dest_dir):
    file_dir = os.path.dirname(file_path)
    filename = os.path.basename(file_path)
    unzip_filename = filename[:-3]
    if not (unzip_filename in os.listdir(dest_dir)):
        os.chdir(file_dir)
        data = subprocess.Popen(['7z', 'e', filename], stdout=subprocess.PIPE)
        output = data.communicate()
        logging.info(output)
        data = subprocess.Popen(['mv', unzip_filename, dest_dir], stdout=subprocess.PIPE)
        output = data.communicate()
        logging.info(output)
    else:
        logging.info('skipping ' + filename + ' already uncompressed!')

def delete_unzipped_file(filename):
    os.chdir(FILES_UNCOMPRESS_LOCATION)
    data = subprocess.Popen(['rm', filename], stdout=subprocess.PIPE)
    output = data.communicate()
    logging.info(output)

def split_file(file_path, dest_dir):
    file_dir = os.path.dirname()
    filename = os.path.basename()
    os.chdir(file_dir)
    data = subprocess.Popen(['mv', filename, dest_dir], stdout=subprocess.PIPE)
    output = data.communicate()
    logging.info(output)
    os.chdir(dest_dir)

    #csplit --prefix  hist2-_part_ --digits=5 enwiki-20201201-pages-meta-history2.xml-p151386p151573 '/<page>$/' '{*}'

def get_elapsed_time(start_time):
    cur_time = datetime.datetime.now()
    return (cur_time - start_time).total_seconds()


if __name__ == '__main__':

    start_time = datetime.datetime.now()
    all_dump_files_url_list = download_dump_files.read_url_list(DOWNLOAD_URL_LIST_FILE)

    # > python3 -m preprocessing.control <start_index> <stop_index>
    # > python3 -m preprocessing.control 10 20

    start_index = int(sys.argv[1])
    stop_index = int(sys.argv[2])

    log_dir = APPLICATION_LOGS_LOCATION
    log_filename = start_time.isoformat().replace(":", "-").replace(".", "-") + "_run_" + sys.argv[1] + "_" + sys.argv[2] + "log.txt"
    numeric_level = getattr(logging, 'INFO', None)
    logging.basicConfig(filename = os.path.join(log_dir, log_filename), level=numeric_level)


    logging.info('downloading files')
    download_dump_files.download_files(all_dump_files_url_list[start_index-1: stop_index], DOWNLOAD_FILES_LOCATION)
    logging.info(str(get_elapsed_time(start_time)) + 's - files downloaded')

    for dump_file in all_dump_files_url_list[start_index-1: stop_index]:
        filename = os.path.basename(dump_file)
        logging.info(str(get_elapsed_time(start_time)) + 's - unzipping ' + filename)
        unzip_file(os.path.join(DOWNLOAD_FILES_LOCATION, filename), FILES_UNCOMPRESS_LOCATION)
        logging.info(str(get_elapsed_time(start_time)) + 's - unzipped ' + filename)
        filename = filename[:-3]
        logging.info(str(get_elapsed_time(start_time)) + 's - init transform to df ' + filename)
        rev_df = strip_text_and_extract_fields_pagelevel.strip_text_get_df(os.path.join(FILES_UNCOMPRESS_LOCATION, filename))
        logging.info(str(get_elapsed_time(start_time)) + 's - completed transform to df ' + filename)

        strip_text_and_extract_fields_pagelevel.revisions_df_to_csv(rev_df, os.path.join(FILES_CSV_LOCATION, filename + "_d_wikifields.csv"))
        logging.info(str(get_elapsed_time(start_time)) + 's - completed wiki_fields_csv ' + filename)
        delete_unzipped_file(filename)
        logging.info(str(get_elapsed_time(start_time)) + 's - deleted uncompressed dump file' + filename)