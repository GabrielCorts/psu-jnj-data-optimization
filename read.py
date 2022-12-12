import pandas as pd
import glob as glob
import csv
import col_types

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

    for file in files:
        df_vec.append(pd.read_csv(file, sep='|', dtype=col_types.types_dict, parse_dates=col_types.parse_dates))
    
    if not df_vec:
        return pd.DataFrame()

    df = pd.concat(df_vec)
    df = df.reset_index(drop=True)
    return df

def readDelta(files: list[str]):
    delta_df_vec = []

    #Lê arquivos delta e armazena num vetor
    for file in files:
        df = pd.read_csv(file, sep='|', dtype=col_types.types_dict, parse_dates=col_types.parse_dates)
        df['delta'] = -2
        df['poId'] = df['poNumber'] + df['poLineNumber'] + df['splitLineNumber']
        delta_df_vec.append(df)


    #Lê arquivos já accruados e seleciona as POs raws (que não são frutos de um aggregate)
    accruedDataJnJ_df = pd.read_csv('./data/accruedDataJnJ.csv', sep='|', dtype=col_types.types_dict, parse_dates=col_types.parse_dates)
    raw_df = accruedDataJnJ_df.loc[(accruedDataJnJ_df['delta'] == -1) & (accruedDataJnJ_df['isValid'] == True)]

    #Preenche as POs do delta com novos IDs (começando do último)
    last_id = accruedDataJnJ_df['id'].max()
    delta_df = pd.concat(delta_df_vec)
    delta_df.insert(0, 'id', range(last_id + 1, last_id + len(delta_df) + 1))

    delta_raw_df_vec = [delta_df, raw_df]

    #Concatena todos data frames e cria coluna de poId para identificar qual PO foi editada ou se é PO nova
    if delta_df_vec:
        delta_raw_df = pd.concat(delta_raw_df_vec)
        delta_raw_df = delta_raw_df.reset_index(drop=True)
        delta_raw_df['poId'] = delta_raw_df['poNumber'] + delta_raw_df['poLineNumber'] + delta_raw_df['splitLineNumber']
        return [accruedDataJnJ_df, delta_raw_df]

    else:
        return []