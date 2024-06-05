
'''
    Código desarrollado en Python 3.12.3.
    Será necesario tener instaladas la librería pandas
'''

#Importación de librerías
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#Descarga de warnings
import warnings
warnings.filterwarnings('ignore')

def carga_xlsx(file_path):
    '''Lectura de ficheros de datos en formato xlsx con extracción de su información básica. 
        Toma un único argumento en formato string con el path al archivo.
        Devuelve el describe del archivo una vez cargado.
    '''
    df = pd.read_excel(file_path, dtype=str)

    return df


def valid_invoices(df, check_cols):
    ''' Función que aplica los criterios minimos que debe tener una factura para que sea considerada validas. 
        Parte de un df y devuelve un df con las facturas que sí cumplen los requisitos
    '''
    #Comprobación de que todos los campos tiene datos
    new_df = df.dropna(subset = check_cols)
    #Comprobamos que no existen facturas con NumeroFactura duplicados, en caso de ocurrir se quedará la primera.
    new_df = new_df.drop_duplicates(subset=['NumeroFactura'], keep='first')
    new_df = new_df.reset_index(drop=True)
    if new_df.shape[0] < df.shape[0]:
        print('El archivo contenía ' + str(df.shape[0]-new_df.shape[0]) + ' facturas no validas, por favor ejecute la función invalid_invoices.')
    else:
        print('Todas las facturas del archivo son válidas.')

    return new_df


def invalid_invoices(df, check_cols):
    ''' Función que aplica los criterios minimos que debe tener una factura para que sea considerada validas. 
        Parte de un df y devuelve un df con las facturas que no cumplen los requisitos
    '''
    #Lista de columnas a checkear que contienen información
    check_nulls = ['NumeroFactura','Emisor','RazonSocialEmisor','Receptor','RazonSocialReceptor','FechaFactura','Importe', 'Moneda','Contrato', 'Origen','ServicioFacturado']
    #DF auxiliar para comprobar que campos no tiene datos.
    aux_df = df.dropna(subset = check_nulls)
    ##DF auxiliar para comprobar que no hay NumeroFactura duplicados
    aux_df = aux_df.drop_duplicates(subset=['NumeroFactura'], keep='first')
    aux_lst = list(aux_df['NumeroFactura'])
    #DF con facturas erróneas por algún motivo
    new_df = df[~df['NumeroFactura'].isin(aux_lst)]
    new_df = new_df.reset_index(drop=True)
    print('El archivo contenía ' + str(new_df.shape[0]) + ' facturas no validas')

    return new_df


def global_info(df):
    ''' Funcion que proporciona datos agregados globales del fichero del dataframe de facturas válidas
        Parte del df con las facturas válidas y devuelve un df con información agregada
    '''
    #Calculo numero facturas
    a = df['NumeroFactura'].nunique()
    #Calculo de clientes
    b = df['RazonSocialEmisor'].nunique()
    #Facturacion total
    c = str(df['Importe'].astype(float).sum().round(3)) + 'EUR'
    #Construccion del DF final
    new_df = pd.DataFrame(data={'NumeroFacturasValidas':a, 'NumeroClientes':b,'FacturacionTotal':c}, index=[0])

    return new_df


def client_info(df):
    '''Funcion que proporciona datos agregados por cada cliente del fichero del dataframe de facturas válidas
       Parte del df con las facturas válidas y devuelve un df con información agregada
    '''
    final_df = pd.DataFrame(columns=['RazonSocialEmisor', 'Emisor ','NumeroOrigenes', 'NumServiciosFacturados', 'NumeroFacturas','FacturacionTotal'])
    #Clientes en el fichero
    clients = list(df['RazonSocialEmisor'].value_counts().keys())
    #Iteramos sobre los clientes
    for i in clients:
        #flitramos datos del cliente:
        df_aux = df[df['RazonSocialEmisor']== i]     
        #Extración datos asociados
        d = df_aux['Emisor'].unique()
        #Selección del Origen
        e = df_aux['Origen'].nunique()
        #Selección del ServicioFacturado
        f = df_aux['ServicioFacturado'].nunique()
        #Calculo de numero facturas
        g = df_aux.shape[0]
        #Facturacion total
        h = str(df_aux['Importe'].astype(float).sum().round(3)) + 'EUR'
        #Creación del dataframe
        new_df = pd.DataFrame(data={'RazonSocialEmisor':i, 'Emisor ':d,'NumeroOrigenes':e, 'NumServiciosFacturados':f, 'NumeroFacturas':g,'FacturacionTotal':h})
        final_df = pd.concat([final_df, new_df])
             
    return final_df


def combcols(df, new_col, cols):
    '''Funcion que proporciona combina los textos de varias columnas en una nueva columna
       Requier un df, el nombre de la nueva columna (new_col) y la lista de columnas a combiar (cols)
    '''
    df = df.astype(str)
    df[new_col] = df[cols].agg('_'.join, axis=1)

    return df


def extract_df_from_json_src(df, col, deal_col):
    """
    Función que itera a través de las filas de un dataframe, buscando dos columnas. Las cuales contienen el ID del deal (deal_col) y el sub-flujo correspondiente. De estas extrae los valores y:
    1) Convierte los valores del json de flujos en un objeto dataframe
    2) Crea una nueva columna para el dataframe creado (Deal_Id), con el valor que ha extraído de deal_col para la fila que corresponde
    3) Asigna este dataframe a una lista
    4) Cuando termina de procesar todas las filas y generado todos los dataframes, concatena los dataframes en uno solo (ignorando el Index)
    Esta función devuelve el dataframe generado en el paso 4)
    """
    #Creación df vacio
    dataframe_list =[]
    #Iterador por los ids de deals del df
    for index, row in df.iterrows():
        deal = row[deal_col]
        extractable_json = row[col]
        print('Generando dataframe para deal: ', deal)
        #apertura del json
        try:
            extractable_json = json.loads(extractable_json)
        #apertura del json si mantiene estructura de entrecomillado erroneo
        except:
            #print("""WARN. El archivo .json está incorrectamente formateado. Sustituyendo ' por "... """)
            extractable_json = json.loads(extractable_json.replace("'", '"'))
        #conversion del json a df
        aux_df = pd.DataFrame(extractable_json)
        aux_df['Deal_Id'] =  [deal for i in range(aux_df.shape[0])]
        #creacion de lista de los dfs que habrá que concatenar
        dataframe_list.append(aux_df)
    #union de los dfs de cada deal en un df global
    print('Concatenando dataframes generados...')
    new_df = pd.concat(dataframe_list, ignore_index= True)
    #reordenación columnas new_df
    new_df = new_df[['Deal_Id','Id','Date','Amount']]
    new_df = new_df.rename(columns={'Id':'SubFlow_Id','Amount':'Subcantidad'})

    return new_df


def flows_info(df):
    '''Funcion que proporciona la información agregada de los subflows del analisis, junto con a que Deal pertenecen
    '''
    new_df = df.groupby('Deal_Id').agg({'SubFlow_Id':'count','Date':'unique', 'Subcantidad':'sum', 'SubFlow_Id':'unique'}).reset_index()
    new_df['N_SubFlows'] = new_df['SubFlow_Id'].apply(lambda x: len(x))
    new_df = new_df[['Deal_Id','SubFlow_Id', 'N_SubFlows', 'Date',	'Subcantidad']]
    new_df = new_df.rename(columns={'SubFlow_Id':'Subflows asociados', })
    return new_df









