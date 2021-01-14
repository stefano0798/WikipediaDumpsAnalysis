from bs4 import BeautifulSoup
import os
import logging
import pandas
import numpy as np

def contributor_details(contributor_tag):
    contributor = {}
    if contributor_tag.username is not None:
        contributor['username'] = contributor_tag.username.text
        contributor['user_id'] = contributor_tag.id.text
    elif contributor_tag.ip is not None:
        contributor['ip'] = contributor_tag.ip.text
    return contributor

def extract_fields_from_xml(dump_rmtext_file_path, wiki_fields_as_csv_file):
    with open(dump_rmtext_file_path) as dump_rmtext_file:
        dump_rmtext_data = dump_rmtext_file.read()
    stripped_soup = BeautifulSoup(dump_rmtext_data, 'xml')

    revisions_df = pandas.DataFrame()
    revisions_count = []

    all_page_tags = stripped_soup.find_all('page')
    i = 0
    for page in all_page_tags:
        if i%500 == 0:
            print(i, " pages parsed")
        i = i + 1
        page_id = page.find('id').text
        page_title = page.find('title').text
        page_ns = page.find('ns').text
        all_revisions = page.find_all('revision')
        revisions_count.append(len(all_revisions))

        rev_attr_list = []
        for rev in all_revisions:
            rev_rec = {}
            rev_attributes = ['id', 'timestamp', 'model', 'format', 'sha1']
            for attr in rev_attributes:
                rev_rec[attr] = rev.find(attr).text
            #     print(rev.find('contributor'))
            contributor_det = contributor_details(rev.find('contributor'))
            if 'user_id' in contributor_det.keys():
                rev_rec['contributor_user_id'] = contributor_det['user_id']
                rev_rec['contributor_username'] = contributor_det['username']
            elif 'ip' in contributor_det.keys():
                rev_rec['contributor_ip'] = contributor_det['ip']
            rev_rec['text_size'] = str(rev.find('text')['bytes'])
            rev_attr_list.append(rev_rec)

        rev_df = pandas.DataFrame.from_records(rev_attr_list)
        rev_df.rename(columns={'id': 'rev_id'}, inplace=True)
        rev_df['page_id'] = page_id
        rev_df['page_title'] = page_title
        rev_df['page_ns'] = page_ns
        revisions_df = pandas.concat([revisions_df, rev_df])

    average_rev_count = np.mean(revisions_count)
    min_rev_count = np.min(revisions_count)
    max_rev_count = np.max(revisions_count)
    logging.info("rev count stats:")
    logging.info("min: " +  str(min_rev_count))
    logging.info("avg: " + str(average_rev_count))
    logging.info("max: " + str(max_rev_count))

    logging.info(str(len(all_page_tags)) + "pages converted to dataframe")
    logging.info(str(len(revisions_df)) + "total number of revisions in this file")
    logging.info("writing dataframe to " + wiki_fields_as_csv_file)

    revisions_df.to_csv(wiki_fields_as_csv_file,
                     columns = ['page_id', 'page_title', 'page_ns', 'rev_id', 'timestamp',
                                'model', 'format', 'text_size', 'sha1',
                                'contributor_ip', 'contributor_user_id', 'contributor_username'])

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        logging.error("usage: wiki_dump_to_dataframe.py <IN_FILE> <OUT_FILE>")
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    extract_fields_from_xml(in_file, out_file)
