import os
import logging
import pandas
import numpy as np
from lxml import etree
import xml.etree.ElementTree as ET

import datetime

ns = {'wiki_ns' : 'http://www.mediawiki.org/xml/export-0.10/'}

def contributor_details(contributor_tag):
    contributor = {}
    if contributor_tag.find('wiki_ns:username',ns) is not None:
        contributor['username'] = contributor_tag.find('wiki_ns:username',ns).text
        contributor['user_id'] = contributor_tag.find('wiki_ns:id', ns).text
    elif contributor_tag.find('wiki_ns:ip',ns) is not None:
        contributor['ip'] = contributor_tag.find('wiki_ns:ip',ns).text
    else:
        contributor['ip'] = 'empty'
    return contributor

def extract_fields_from_xml(dump_rmtext_data):
    ns = {'wiki_ns': 'http://www.mediawiki.org/xml/export-0.10/'}

    logging.info(datetime.datetime.now().isoformat())
    logging.info("in extract_fields - starting etree fromstring")

    etree_start = datetime.datetime.now()
    rmtext_parsed = ET.fromstring(dump_rmtext_data)
    etree_done = datetime.datetime.now()
    logging.info(datetime.datetime.now().isoformat())
    logging.info("in extract_fields - etree fromstring done")
    logging.info("time taken: " + str((etree_done-etree_start).total_seconds()))

    revisions_df = pandas.DataFrame()
    revisions_count = []
    all_page_tags = rmtext_parsed.findall('wiki_ns:page', ns)

    i = 0
    for page in all_page_tags:
        if i % 500 == 0 and len(all_page_tags) > 10:
            cur_time = datetime.datetime.now()
            print(i, " pages parsed")
            logging.info(cur_time.isoformat() + "   " + str(i) + " pages parsed")
        i = i + 1
        page_id = page.find('wiki_ns:id', ns).text
        page_title = page.find('wiki_ns:title', ns).text
        page_ns = page.find('wiki_ns:ns', ns).text
        all_revisions = page.findall('wiki_ns:revision', ns)
        revisions_count.append(len(all_revisions))

        rev_attr_list = []
        for rev in all_revisions:
            rev_rec = {}
            rev_attributes = ['id', 'timestamp', 'model', 'format', 'sha1']
            for attr in rev_attributes:
                rev_rec[attr] = rev.find('wiki_ns:' + attr, ns).text
            parent_id = rev.find('wiki_ns:parentid', ns)
            if parent_id is None:
                rev_rec['parent_revid'] = 'na'
            else:
                rev_rec['parent_revid'] = parent_id.text
            contributor_det = contributor_details(rev.find('wiki_ns:contributor', ns))
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
            rev_rec['text_size'] = str(rev.find('wiki_ns:text', ns).attrib['bytes'])
            rev_attr_list.append(rev_rec)

        rev_df = pandas.DataFrame.from_records(rev_attr_list)
        rev_df.rename(columns={'id': 'rev_id'}, inplace=True)
        rev_df['page_id'] = page_id
        rev_df['page_title'] = page_title
        rev_df['page_ns'] = page_ns
        revisions_df = pandas.concat([revisions_df, rev_df])
    return revisions_df


def revisions_df_to_csv(revisions_df, wiki_fields_as_csv_file):
    revisions_df.to_csv(wiki_fields_as_csv_file, index=False,
                        columns=['page_id', 'page_title', 'page_ns', 'rev_id', 'timestamp', 'parent_revid',
                                 'model', 'format', 'text_size', 'sha1',
                                 'contributor_ip', 'contributor_user_id', 'contributor_username'])
    logging.info("writing dataframe to " + wiki_fields_as_csv_file)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        logging.error("usage: wiki_dump_to_dataframe.py <IN_FILE> <OUT_FILE>")
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    with open(in_file) as dump_rmtext_file:
        dump_rmtext_data = dump_rmtext_file.read()
    revisions = extract_fields_from_xml(dump_rmtext_data)
    revisions_df_to_csv(revisions, out_file)
