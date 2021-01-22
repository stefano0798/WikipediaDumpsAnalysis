import os
import subprocess
import sys

def download_files(url_list, dest_dir):
    os.chdir(dest_dir)
    for url in url_list:
        filename = os.path.basename(url)
        if filename not in os.listdir(dest_dir):
            print('downloading', filename)
            data = subprocess.Popen(['wget', url], stdout=subprocess.PIPE)
            output = data.communicate()
            print(output)

def read_url_list(url_list_file):
    with open(url_list_file, 'r') as read_file:
        read_file_text = read_file.read()
    url_list = read_file_text.split('\n')
    return url_list

if __name__ == "__main__":
    # > python3 -m preprocessing.download_dump_files <url_list_file> <start_index> <stop_index> <dest_dir>

    url_list_file = sys.argv[1]
    start_index = int(sys.argv[2])
    stop_index = int(sys.argv[3])
    dest_dir = sys.argv[4]

    url_list = read_url_list(url_list_file)
    urls_to_download = url_list[start_index - 1: stop_index]

    download_files(urls_to_download, dest_dir)