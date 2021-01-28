import os

DOWNLOAD_URL_LIST_FILE = '/home/s2575760/project/wiki_bigdata/resources/wikidumps_source_download_list.txt'
preprocessed_files_path = '/home/s2575760/project/wiki_preprocessed'
uncompressed_files_path = '/home/s2575760/project/wiki_dump_files/preproc_inter/unzipped'

with open(DOWNLOAD_URL_LIST_FILE) as download_url_file:
    download_url_list = download_url_file.read().split('\n')
download_filename_list = list(map(lambda x: os.path.basename(x)[:-3], download_url_list))

preprocessed_files = os.listdir(preprocessed_files_path)

def is_preprocessed(filename):
    for preprocessed_filename in preprocessed_files:
        if filename in preprocessed_filename:
            return True
    return False

for i in range(len(download_filename_list)):
    filename = download_filename_list[i]
    if is_preprocessed(filename):
        print(str(i+1) + " DONE! " + filename)
    else:
        print(str(i+1) + " TODO  " + filename)