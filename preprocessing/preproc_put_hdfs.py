import os
import subprocess

DOWNLOAD_URL_LIST_FILE = '/home/s2575760/project/wiki_bigdata/resources/wikidumps_source_download_list.txt'
preprocessed_files_path = '/home/s2575760/project/wiki_preprocessed'
uncompressed_files_path = '/home/s2575760/project/wiki_dump_files/preproc_inter/unzipped'
PREPROCESSED_FILES_HDFS_LOCATION = '/user/s2575760/project/data/enwiki-202012010-pages-meta-history'


data = subprocess.Popen(['hdfs', 'dfs', '-ls', PREPROCESSED_FILES_HDFS_LOCATION], stdout=subprocess.PIPE)
output = data.communicate()

hdfs_ls_output = output[0].decode("utf-8")
print(hdfs_ls_output)

preprocessed_files = os.listdir(preprocessed_files_path)
os.chdir(preprocessed_files_path)
for pr_file in preprocessed_files:
    if pr_file not in hdfs_ls_output:
        print("copying to hdfs: "  + pr_file)
        data = subprocess.Popen(['hdfs', 'dfs', '-put', pr_file, PREPROCESSED_FILES_HDFS_LOCATION], stdout=subprocess.PIPE)
        output = data.communicate()
        print(output)

