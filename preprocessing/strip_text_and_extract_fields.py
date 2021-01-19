from bs4 import BeautifulSoup
import lxml
from lxml import etree
import os
import logging
import numpy as np
import datetime

def remove_text_from_revision_lxml(rev_text):
    rev_parsed = etree.fromstring(rev_text)
    rev_parsed.find('text').text = ' '
    return str(etree.tostring(rev_parsed))

def remove_text_from_revision(rev_text):
    rev_parsed = BeautifulSoup(rev_text, 'xml')
    rev_text_tag = rev_parsed.find('text')
    if rev_text_tag is not None:
        rev_text_tag.string = " "
    else:
        print(rev_text)
    return str(rev_parsed)

def strip_text(raw_dump_file_path, dump_rmtext_file_path):
    filename = os.path.basename(raw_dump_file_path)
    i = 0
    num_revisions = 0
    with open(raw_dump_file_path, 'r') as raw_dump_file:
        with open(dump_rmtext_file_path, 'w') as dump_rmtext_file:
            inside_revision_tag = False
            current_revision_text = ""
            for line in raw_dump_file:
                i = i + 1
                if i % 10000000 == 0:
                    print("rmtext ", filename, i, "lines read")

                if inside_revision_tag:
                    current_revision_text = current_revision_text + line + '\n'
                    if '</revision>' in line:
                        try:
                            rmtext = remove_text_from_revision_lxml(current_revision_text)
                        except Exception:
                            rmtext = remove_text_from_revision(current_revision_text)
                        dump_rmtext_file.write(rmtext)
                        num_revisions = num_revisions + 1
                        current_revision_text = ""
                        inside_revision_tag = False
                else:
                    if '<revision>' in line:
                        current_revision_text = current_revision_text + line + '\n'
                        inside_revision_tag = True
                    else:
                        dump_rmtext_file.write(line)
    print(filename, "number of revisions read: ", num_revisions)




if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        logging.error("usage: strip_text_from_raw_dump.py <IN_FILE> <OUT_FILE>")
    # print(sys.argv)
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    strip_text(in_file, out_file)
