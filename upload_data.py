import pandas as pd
from mysql import connector
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
DB = ['CineColombia', 'CineMark', 'Procinal', 'UniCine']

for db in DB:
    conn = connector.connect(user=username,
                             password=password,
                             host=host,
                             database=db)
    if (db == 'UniCine'):
        df_visualizaciones = pd.read_sql(
            '''SELECT * FROM visualizaciones;''', conn)
        filename = db+'_' + 'visualizaciones' + '.json'
        df_visualizaciones.to_json(
            index=False, date_format='%Y-%M-%d', path_or_buf=f'data/{filename}')
        print(f'df_visualizaciones de {db} creado exitosamente')
        df_usuarios = pd.read_sql('''SELECT * FROM usuarios;''', conn)
        filename = db+'_' + 'usuarios' + '.json'
        df_usuarios.to_json(index=False, date_format='%Y-%M-%d',
                            path_or_buf=f'data/{filename}')
        print(f'df_usuarios de {db} creado exitosamente')
        df_peliculas = pd.read_sql('''SELECT * FROM peliculas;''', conn)
        filename = db+'_' + 'peliculas' + '.json'
        df_peliculas.to_json(index=False, date_format='%Y-%M-%d',
                             path_or_buf=f'data/{filename}')
    else:
        df_visualizaciones = pd.read_sql(
            '''SELECT * FROM visualizaciones;''', conn)
        filename = db+'_' + 'visualizaciones' + '.csv'
        df_visualizaciones.to_csv(
            index=False, date_format='%Y-%M-%d', path_or_buf=f'data/{filename}')
        print(f'df_visualizaciones de {db} creado exitosamente')
        df_usuarios = pd.read_sql('''SELECT * FROM usuarios;''', conn)
        # filename = db+'_' + str(datetime.now().date()) + ' _usuarios' + '.csv'
        filename = db+'_' + 'usuarios' + '.csv'
        df_usuarios.to_csv(index=False, date_format='%Y-%M-%d',
                           path_or_buf=f'data/{filename}')
        print(f'df_usuarios de {db} creado exitosamente')
        df_peliculas = pd.read_sql('''SELECT * FROM peliculas;''', conn)
        # filename = db+'_' + str(datetime.now().date()) + '_peliculas' + '.csv'
        filename = db+'_' + 'peliculas' + '.csv'
        df_peliculas.to_csv(index=False, date_format='%Y-%M-%d',
                            path_or_buf=f'data/{filename}')
    print(f'df_peliculas de {db} creado exitosamente')
