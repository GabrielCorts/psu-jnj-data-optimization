import pandas as pd

#Função para salvar as POs processadas em um novo arquivo .csv
#Salvando arquivos
def saveFile(files: list[str], df: pd.DataFrame, path: str):
    df.to_csv('./data/accruedDataJnJ.csv', index=False, sep='|')

    with open(f"./data/{path}.txt", "w") as txt_file:
        for line in files:
            txt_file.write(line + "\n")