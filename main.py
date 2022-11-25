import glob as glob
import csv
from read import readPOs
from process import processPOs
from save import saveFile

files_processed = []
if glob.glob('./data/files_processed.txt'):
    with open('./data/files_processed.txt', 'r') as file:
        reader = csv.reader(file, delimiter='\n')
        for row in reader:
            files_processed += row
            
files_full = [file for file in glob.glob('./data/full/*.txt') if file not in files_processed]
files_delta = [file for file in glob.glob('./data/delta/*.txt') if file not in files_processed]
files = files_full + files_delta

df = readPOs(files=files_full)

if not df.empty:
    final_df = processPOs(df)
    saveFile(files=files_full, df = final_df)
    print('Files processed successfully')
else:
    print('No files to process')