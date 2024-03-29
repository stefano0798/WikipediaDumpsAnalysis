import subprocess
import sys
import datetime
import os
import logging
from preprocessing import download_dump_files
from preprocessing import strip_text_and_extract_fields
from preprocessing import wiki_dump_to_dataframe_lxml


DOWNLOAD_URL_LIST_FILE = '/home/s2575760/project/wiki_bigdata/resources/wikidumps_source_download_list.txt'
DOWNLOAD_FILES_LOCATION = '/home/s2575760/project/wiki_dump_files/full_page_history_dumps'
FILES_UNCOMPRESS_LOCATION = '/home/s2575760/project/wiki_dump_files/preproc_inter/unzipped'
FILES_SPLIT_LOCATION = '/home/s2575760/project/wiki_dump_files/preproc_inter/split'
FILES_RMTEXT_LOCATION = '/home/s2575760/project/wiki_dump_files/preproc_inter/rmtext'
FILES_CSV_LOCATION = '/home/s2575760/project/wiki_preprocessed'

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

    start_index = int(sys.argv[1])
    stop_index = int(sys.argv[2])

    log_dir = APPLICATION_LOGS_LOCATION
    log_filename = start_time.isoformat().replace(":", "-").replace(".", "-") + "_unzip_only_" + sys.argv[1] + "_" + sys.argv[2] + "log.txt"
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