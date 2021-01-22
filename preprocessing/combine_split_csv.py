import pandas
import os
import sys

if __name__ == "__main__":
    dir_path = sys.argv[1]
    wiki_fields_csv_files = list(filter(lambda x: 'wiki_fields' in x,  os.listdir(dir_path)))
    print(str(len(wiki_fields_csv_files)) + "files found")

    revisions_combined = pandas.DataFrame()
    for csv_part_file in wiki_fields_csv_files:
        revision_part = pandas.read_csv(os.path.join(dir_path, csv_part_file))
        revisions_combined = pandas.concat([revisions_combined, revision_part])
    dir_name = os.path.basename(dir_path)
    combine_csv_file = "enwiki_" + dir_name + ".csv"
    revisions_combined.to_csv(os.path.join(dir_path, combine_csv_file))