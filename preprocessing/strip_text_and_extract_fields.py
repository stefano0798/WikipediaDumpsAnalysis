from bs4 import BeautifulSoup
import lxml
from lxml import etree
import xml.etree.ElementTree as ET

import os
import logging
import numpy as np
import datetime

def remove_text_from_revision_lxml(rev_text):
    rev_parsed = etree.fromstring(rev_text)
    rev_parsed.find('text').text = ' '
    return str(etree.tostring(rev_parsed))

def remove_text_from_revision_pyxml(rev_text):
    rev_parsed = ET.fromstring(rev_text)
    rev_parsed.find('text').text = ' '
    return str(ET.tostring(rev_parsed))

def remove_text_from_revision(rev_text):
    rev_parsed = BeautifulSoup(rev_text, 'xml')
    rev_text_tag = rev_parsed.find('text')
    #TODO seems like this else is not required
    # confirm and remove
    if rev_text_tag is not None:
        rev_text_tag.string = " "
    else:
        print(rev_text)
    return str(rev_parsed)

def strip_text(raw_dump_file_path):
    rev_parse_count = {'soup': 0, 'lxml': 0, 'pyxml': 0}
    rmtext_str = ''
    filename = os.path.basename(raw_dump_file_path)
    i = 0
    num_revisions = 0
    with open(raw_dump_file_path, 'r') as raw_dump_file:
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
                        rev_parse_count['lxml'] = rev_parse_count['lxml'] + 1
                    except Exception:
                        try:
                            rmtext = remove_text_from_revision_pyxml(current_revision_text)
                            rev_parse_count['pyxml'] = rev_parse_count['pyxml'] + 1
                        except Exception:
                            rmtext = remove_text_from_revision(current_revision_text)
                            rev_parse_count['soup'] = rev_parse_count['soup'] + 1

                    rmtext_str = rmtext_str + rmtext
                    num_revisions = num_revisions + 1
                    current_revision_text = ""
                    inside_revision_tag = False
            else:
                if '<revision>' in line:
                    current_revision_text = current_revision_text + line + '\n'
                    inside_revision_tag = True
                else:
                    rmtext_str = rmtext_str + line
    logging.info(filename + " number of revisions read: " + str(num_revisions))
    logging.info(str(rev_parse_count))
    return rmtext_str



if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        logging.error("usage: strip_text_from_raw_dump.py <IN_FILE> <OUT_FILE>")
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    rmtext = strip_text(in_file)
