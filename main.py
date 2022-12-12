import glob as glob
import csv
from read import readPOs, readDelta
from process import processFullPOs, processDeltaPOs
from save import saveFile

files_processed = []
deltas_processed = []
if glob.glob('./data/files_processed.txt'):
    with open('./data/files_processed.txt', 'r') as file:
        reader = csv.reader(file, delimiter='\n')
        for row in reader:
            files_processed += row

if glob.glob('./data/deltas_processed.txt'):
    with open('./data/deltas_processed.txt', 'r') as file:
        reader = csv.reader(file, delimiter='\n')
        for row in reader:
            deltas_processed += row
            
files_full = [file for file in glob.glob('./data/full/*.txt') if file not in files_processed]
files_delta = [file for file in glob.glob('./data/delta/test_delta*.txt') if file not in deltas_processed]
files = files_full + files_delta

#Full Files-------------------------------------------------------------------------------
df = readPOs(files=files_full)

if not df.empty:
    final_df = processFullPOs(df)
    saveFile(files=files_full, df = final_df, path='files_processed')
    print('Full files processed successfully')
else:
    print('No full files to process')

#Delta Files------------------------------------------------------------------------------
df_vec = readDelta(files=files_delta)

if df_vec:
    accruedDataJnJ_df = df_vec[0]
    delta_raw_df = df_vec[1]

    final_df = processDeltaPOs(delta_raw_df=delta_raw_df, output_df=accruedDataJnJ_df)
    saveFile(files=files_full, df = final_df, path='deltas_processed')
    print('Delta files processed successfully')
else:
    print('No delta files to process')