from bs4 import BeautifulSoup
import lxml
# from lxml import etree
import xml.etree.ElementTree as ET

import os
import logging
import numpy as np
import datetime
import pandas

ns = {'wiki_ns' : 'http://www.mediawiki.org/xml/export-0.10/'}

# def remove_text_from_revision_lxml(rev_text):
#     rev_parsed = etree.fromstring(rev_text)
#     rev_parsed.find('text').text = ' '
#     return str(etree.tostring(rev_parsed))
#
# def remove_text_from_revision_pyxml(rev_text):
#     rev_parsed = ET.fromstring(rev_text)
#     rev_parsed.find('text').text = ' '
#     return str(ET.tostring(rev_parsed))
#
# def remove_text_from_revision(rev_text):
#     rev_parsed = BeautifulSoup(rev_text, 'xml')
#     rev_text_tag = rev_parsed.find('text')
#     #TODO seems like this else is not required
#     # confirm and remove
#     if rev_text_tag is not None:
#         rev_text_tag.string = " "
#     else:
#         print(rev_text)
#     return str(rev_parsed)

def contributor_details(contributor_tag):
    contributor = {}
    # if contributor_tag.find('wiki_ns:username',ns) is not None:
    if contributor_tag.find('username',ns) is not None:
        # contributor['username'] = contributor_tag.find('wiki_ns:username',ns).text
        contributor['username'] = contributor_tag.find('username',ns).text
        # contributor['user_id'] = contributor_tag.find('wiki_ns:id', ns).text
        contributor['user_id'] = contributor_tag.find('id', ns).text
    # elif contributor_tag.find('wiki_ns:ip',ns) is not None:
    elif contributor_tag.find('ip',ns) is not None:
        # contributor['ip'] = contributor_tag.find('wiki_ns:ip',ns).text
        contributor['ip'] = contributor_tag.find('ip',ns).text
    else:
        contributor['ip'] = 'empty'
    return contributor

def get_page_details(page_stub_text):
    page = ET.fromstring(page_stub_text)
    # page_id = page.find('wiki_ns:id', ns).text
    # page_title = page.find('wiki_ns:title', ns).text
    # page_ns = page.find('wiki_ns:ns', ns).text
    page_id = page.find('id', ns).text
    page_title = page.find('title', ns).text
    page_ns = page.find('ns', ns).text
    return {'page_id': page_id, 'page_title': page_title, 'page_ns': page_ns}

def revision_text_to_dict(rev_text):
    rev = ET.fromstring(rev_text)
    rev_rec = {}
    rev_attributes = ['id', 'timestamp', 'model', 'format', 'sha1']
    for attr in rev_attributes:
        # rev_rec[attr] = rev.find('wiki_ns:' + attr, ns).text
        rev_rec[attr] = rev.find(attr, ns).text
    # parent_id = rev.find('wiki_ns:parentid', ns)
    parent_id = rev.find('parentid', ns)
    if parent_id is None:
        rev_rec['parent_revid'] = 'na'
    else:
        rev_rec['parent_revid'] = parent_id.text
    # contributor_det = contributor_details(rev.find('wiki_ns:contributor', ns))
    contributor_det = contributor_details(rev.find('contributor', ns))
    if contributor_det is not None:
        if 'user_id' in contributor_det.keys():
            rev_rec['contributor_user_id'] = contributor_det['user_id']
            rev_rec['contributor_username'] = contributor_det['username']
            rev_rec['contributor_ip'] = 'na'
        elif 'ip' in contributor_det.keys():
            rev_rec['contributor_username'] = 'na'
            rev_rec['contributor_user_id'] = 'na'
            rev_rec['contributor_ip'] = contributor_det['ip']
    else:
        rev_rec['contributor_username'] = 'empty'
        rev_rec['contributor_ip'] = 'empty'
        rev_rec['contributor_user_id'] = 'empty'
    # rev_rec['text_size'] = str(rev.find('wiki_ns:text', ns).attrib['bytes'])
    rev_rec['text_size'] = str(rev.find('text', ns).attrib['bytes'])
    return rev_rec

def strip_text_get_df(raw_dump_file_path):
    # rev_parse_count = {'soup': 0, 'lxml': 0, 'pyxml': 0}
    filename = os.path.basename(raw_dump_file_path)
    i = 0 # lines read
    num_revisions = 0
    with open(raw_dump_file_path, 'r') as raw_dump_file:

        # state variables
        inside_page_tag = False
        page_stub_tag = ""
        inside_revision_tag = False
        current_revision_text = ""
        page_info_read = False

        #
        rev_df = pandas.DataFrame()
        for line in raw_dump_file:
            i = i + 1
            if i % 10000000 == 0:
                print("rmtext ", filename, i, "lines read")

            if inside_page_tag:
                # at end of page
                if '</page>' in line:

                    # convert revision_records to dataframe
                    cur_rev_df = pandas.DataFrame.from_records(rev_attr_list)
                    cur_rev_df.rename(columns={'id': 'rev_id'}, inplace=True)
                    cur_rev_df['page_id'] = page_info['page_id']
                    cur_rev_df['page_title'] = page_info['page_title']
                    cur_rev_df['page_ns'] = page_info['page_ns']
                    # append to main rev_df
                    rev_df = pandas.concat([rev_df, cur_rev_df])
                    cur_rev_df = None

                    #update state variables
                    inside_page_tag = False
                    page_stub_tag = ""
                    page_info_read = False

                elif inside_revision_tag:
                    current_revision_text = current_revision_text + line + '\n'
                    #at end of revision
                    if '</revision>' in line:
                        # try:
                        # read revision xml - convert to python dict
                        rev_record = revision_text_to_dict(current_revision_text)
                        rev_attr_list.append(rev_record)
                        # rev_parse_count['lxml'] = rev_parse_count['lxml'] + 1
                        # except Exception:
                        #     try:
                        #         rmtext = remove_text_from_revision_pyxml(current_revision_text)
                        #         rev_parse_count['pyxml'] = rev_parse_count['pyxml'] + 1
                        #     except Exception:
                        #         rmtext = remove_text_from_revision(current_revision_text)
                        #         rev_parse_count['soup'] = rev_parse_count['soup'] + 1

                        # update counter
                        num_revisions = num_revisions + 1

                        # update state variables
                        current_revision_text = ""
                        inside_revision_tag = False

                # read page attributes
                elif page_info_read == False and any(list(map(lambda x: x in line, ['<title>', '<ns>', '<id>']))):
                    page_stub_tag = page_stub_tag + line

                else:
                    if '<revision>' in line:
                        current_revision_text = current_revision_text + line + '\n'
                        inside_revision_tag = True
                        if page_info_read == False:
                            page_stub_tag = page_stub_tag + '\n</page>'
                            page_info = get_page_details(page_stub_tag)
                            page_info_read = True
            elif '<page>' in line:
                page_stub_tag = page_stub_tag + line + '\n'
                inside_page_tag = True
                rev_attr_list = []

    logging.info(filename + " number of revisions read: " + str(num_revisions))
    # logging.info(str(rev_parse_count))
    return rev_df


def revisions_df_to_csv(revisions_df, wiki_fields_as_csv_file):
    revisions_df.to_csv(wiki_fields_as_csv_file, index=False,
                        columns=['page_id', 'page_title', 'page_ns', 'rev_id', 'timestamp', 'parent_revid',
                                 'model', 'format', 'text_size', 'sha1',
                                 'contributor_ip', 'contributor_user_id', 'contributor_username'])
    logging.info("writing dataframe to " + wiki_fields_as_csv_file)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        logging.error("usage: strip_text_and_extract_fields_pagelevel.py <IN_FILE> <OUT_FILE>")
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    rev_df = strip_text_get_df(in_file)
    revisions_df_to_csv(rev_df, out_file)
