from bs4 import BeautifulSoup
import os
import logging

def strip_text(raw_dump_file_path, dump_rmtext_file_path):
    with open(raw_dump_file_path, 'r') as raw_dump_file:
        raw_dump_data = raw_dump_file.read()
    raw_dump_soup = BeautifulSoup(raw_dump_data, 'xml')
    all_revisions = raw_dump_soup.find_all('revision')
    for rev in all_revisions:
        text_tag = rev.find('text')
        text_tag.string = " "
    with open(dump_rmtext_file_path, 'w') as dump_rmtext_file:
        dump_rmtext_file.write(str(raw_dump_soup))


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        logging.error("usage: strip_text_from_raw_dump.py <IN_FILE> <OUT_FILE>")
    # print(sys.argv)
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    strip_text(in_file, out_file)
