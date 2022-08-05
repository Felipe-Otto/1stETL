# Impoting Pandas
import pandas as pd

# Reading Files
def read_files(path, name_file, year_date, type_file):
    file = f'{path}{name_file}{year_date}.{type_file}'
    colspecs = [(2, 10),
                (10, 12),
                (12, 24),
                (27, 39),
                (56, 69),
                (69, 82),
                (82, 95),
                (108, 121),
                (152, 170),
                (171, 188)
                ]

    names = ['data_pregao', 'codigo_bdi', 'sigla_acao', 'nome_acao', 'preco_abertura', 'preco_maximo', 'preco_minimo',
             'preco_fechamento', 'quantidade_negocios', 'volume_negocios']

    df = pd.read_fwf(file, colspecs=colspecs, names=names, skiprows=1)

    return df


# Filtering stocks
def filter_stocks(df):
    df = df[df['codigo_bdi'] == 2]
    df = df.drop(['codigo_bdi'], 1)

    return df


#  Adjusting date field
def parse_date(df):
    df['data_pregao'] = pd.to_datetime(df['data_pregao'], format='%Y%m%d')

    return df


# Adjusting values field
def parse_values(df):
    df['preco_abertura'] = (df['preco_abertura'] / 100).astype(float)
    df['preco_maximo'] = (df['preco_maximo'] / 100).astype(float)
    df['preco_minimo'] = (df['preco_minimo'] / 100).astype(float)
    df['preco_fechamento'] = (df['preco_fechamento'] / 100).astype(float)

    return (df)


# Concatenating files
def concat_files(path, name_file, year_date, type_file, final_file):
    for i, y in enumerate(year_date):
        df = read_files(path, name_file, y, type_file)
        df = filter_stocks(df)
        df = parse_date(df)
        df = parse_values(df)

        if i == 0:
            df_final = df
        else:
            df_final = pd.concat([df_final, df])

        df_final.to_csv(f'{path}//{final_file}', index=False)


# Executing ETL progam
year_date = ['2020', '2021', '2022']

path = f'../1stETL/'

name_file = 'COTAHIST_A'

type_file = 'TXT'

final_file = 'all_bovespa.csv'

concat_files(path, name_file, year_date, type_file, final_file)
