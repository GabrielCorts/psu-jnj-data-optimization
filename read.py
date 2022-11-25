import pandas as pd
import glob as glob
import csv

#Função para leitura dos arquivos presentes nas pastas
def readFiles():
    files_processed = []
    if glob.glob('./data/files_processed.txt'):
        with open('./data/files_processed.txt', 'r') as file:
            reader = csv.reader(file, delimiter='\n')
            for row in reader:
                files_processed += row
                
    files_full = [file for file in glob.glob('./data/full/*.txt') if file not in files_processed]
    files_delta = [file for file in glob.glob('./data/delta/*.txt') if file not in files_processed]
    files = files_full + files_delta

#Função para leitura das POs dos arquivos
#Leitura dos Arquivos
def readPOs(files: list[str]):
    df_vec = []
    types_dict = {'poRequisitionerWwid': str, 'poPreparerWwid': str, 'mrc': str}

    for file in files:
        df_vec.append(pd.read_csv(file, sep='|', dtype=types_dict))
    
    if not df_vec:
        return pd.DataFrame()

    df = pd.concat(df_vec)
    df = df.reset_index(drop=True)
    return df