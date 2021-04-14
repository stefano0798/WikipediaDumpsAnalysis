import os
import subprocess

DOWNLOAD_URL_LIST_FILE = '/home/s2575760/project/wiki_bigdata/resources/wikidumps_source_download_list.txt'
# preprocessed_files_path = '/home/s2575760/project/wiki_preprocessed'
PREPROCESSED_FILES_HDFS_LOCATION = '/user/s2575760/project/data/enwiki-202012010-pages-meta-history'
# uncompressed_files_path = '/home/s2575760/project/wiki_dump_files/preproc_inter/unzipped'

with open(DOWNLOAD_URL_LIST_FILE) as download_url_file:
    download_url_list = download_url_file.read().split('\n')
download_filename_list = list(map(lambda x: os.path.basename(x)[:-3], download_url_list))

def hdfs_ls(hdfs_path):
    data = subprocess.Popen(['hdfs', 'dfs', '-ls', hdfs_path], stdout=subprocess.PIPE)
    output = data.communicate()

    hdfs_ls_output = output[0].decode("utf-8")
    return hdfs_ls_output

def is_preprocessed(filename):
    for preprocessed_filename in preprocessed_files_list:
        if filename in preprocessed_filename:
            return True
    return False

def filename_from_ls_row(ls_row):
    ls_row_split = ls_row.split(' ')
    if len(ls_row) > 0:
        return ls_row.split(' ')[-1]
    else:
        return ''

# preprocessed_files = os.listdir(preprocessed_files_path)
preprocessed_files = hdfs_ls(PREPROCESSED_FILES_HDFS_LOCATION)
preprocessed_files_ls_list = preprocessed_files.split('\n')
preprocessed_files_list = list(map(lambda x: filename_from_ls_row(x), preprocessed_files_ls_list))

for i in range(len(download_filename_list)):
    filename = download_filename_list[i]
    if is_preprocessed(filename):
        print(str(i+1) + " DONE! " + filename)
    else:
        print(str(i+1) + " TODO  " + filename)
