import os

preprocessed_files_path = '/home/s2575760/project/wiki_preprocessed'
uncompressed_files_path = '/home/s2575760/project/wiki_dump_files/preproc_inter/unzipped'

preprocessed_files = os.listdir(preprocessed_files_path)
uncompressed_files = os.listdir(uncompressed_files_path)

def is_preprocessed(filename):
    for preprocessed_filename in preprocessed_files:
        if filename in preprocessed_filename:
            return True
    return False

for uncomp_file in uncompressed_files:
    if is_preprocessed(uncomp_file):
        print("DONE! - ", uncomp_file)
        print("removing uncompressed file")
        os.remove(os.path.join(uncompressed_files_path, uncomp_file))