import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor

def clean_month_year(value):
    return value.replace('MES:', '').strip()

def load_and_transform_excel(input_path, start_data_row=11):
    engine = 'xlrd' if input_path.endswith('.xls') else None
    df_month = pd.read_excel(input_path, header=None, engine=engine)
    df = pd.read_excel(input_path, header=start_data_row, engine=engine)
    
    row_values = df_month.iloc[start_data_row - 1].dropna().values
    current_month, current_year = row_values[1].split('/')
    current_month = clean_month_year(current_month)
    current_year = clean_month_year(current_year)
    
    cols = ['DIA', 'HORA', 'ALTURA (m)', 'CAUDAL (m3/seg)',
            'DIA.1', 'HORA.1', 'ALTURA (m).1', 'CAUDAL (m3/seg).1',
            'DIA.2', 'HORA.2', 'ALTURA (m).2', 'CAUDAL (m3/seg).2']
    
    tmp_df = df[cols].copy()
    tmp_df = tmp_df.iloc[:-1]

    new_data = []
    
    for i in range(len(tmp_df)):
        row_values = tmp_df.iloc[i].dropna().values
        
        if len(row_values) == 2:
            try:
                current_month, current_year = row_values[1].split('/')
                current_month = clean_month_year(current_month)
                current_year = clean_month_year(current_year)
            except:
                raise ValueError(f"Error al procesar la fila {i}: {row_values}")
            
        else:
            new_data.append(
                [tmp_df.iloc[i]['DIA'], current_month, current_year,
                 tmp_df.iloc[i]['HORA'], tmp_df.iloc[i]['ALTURA (m)'],
                 tmp_df.iloc[i]['CAUDAL (m3/seg)']])
            
            new_data.append(
                [tmp_df.iloc[i]['DIA.1'], current_month, current_year,
                 tmp_df.iloc[i]['HORA.1'], tmp_df.iloc[i]['ALTURA (m).1'],
                 tmp_df.iloc[i]['CAUDAL (m3/seg).1']])
            
            new_data.append(
                [tmp_df.iloc[i]['DIA.2'], current_month, current_year,
                 tmp_df.iloc[i]['HORA.2'], tmp_df.iloc[i]['ALTURA (m).2'],
                 tmp_df.iloc[i]['CAUDAL (m3/seg).2']])
    
    new_df = pd.DataFrame(
        new_data, 
        columns=['DIA', 'MES', 'AÑO', 'HORA', 'ALTURA (m)', 'CAUDAL (m3/seg)']
        )
    
    new_df['FECHA'] = pd.to_datetime(
        new_df[['DIA', 'MES', 'AÑO']].astype(str).agg('-'.join, axis=1),
        format='%d-%m-%Y', errors='coerce'
        )
    
    new_df = new_df.dropna(subset=['FECHA'])
    new_df = new_df[['FECHA', 'HORA', 'ALTURA (m)', 'CAUDAL (m3/seg)']]
    
    return new_df

def procesar_subcarpeta(subcarpeta_path, carpeta_salida):
    archivos = os.listdir(subcarpeta_path)
    subfolder_files = [f for f in archivos if f.endswith('.xls') or f.endswith('.xlsx')]
    for subfolder_file in subfolder_files:
        subfolder_file_path = os.path.join(subcarpeta_path, subfolder_file)
        codigo_estacion = subfolder_file.split("_")[0]
        output_file_path = os.path.join(
            carpeta_salida,
            f"{codigo_estacion.split('.')[0]}.xlsx"
            )
        procesar_archivos(subfolder_file_path, output_file_path)

def procesar_carpeta(input_folder, path_salida="datos_salida", max_threads=None):
    
    if max_threads is None:
        max_threads = os.cpu_count()  # Use the number of CPUs available
        
    carpeta_salida = crear_carpeta_salida(path_salida)
    dir_files = os.listdir(input_folder)
    has_subfolders = any(os.path.isdir(os.path.join(input_folder, item)) for item in dir_files)
    has_xls_files = any(item.endswith('.xls') for item in dir_files)

    if has_subfolders and not has_xls_files:
        print("Se encontraron solo subcarpetas con archivos xls")
        print("ejecutando procesamiento en cada carpeta\n")
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(procesar_subcarpeta, os.path.join(input_folder, item), carpeta_salida) for item in dir_files if os.path.isdir(os.path.join(input_folder, item))]
            for future in futures:
                future.result()

    elif not has_subfolders and has_xls_files:
        print("Se encontraron solo archivos xls")
        print("ejecutando procesamiento en cada archivo \n")
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(procesar_archivos, os.path.join(input_folder, item), os.path.join(carpeta_salida, f"{item.split('.')[0]}_Long_Format.xlsx")) for item in dir_files if item.endswith('.xls') or item.endswith('.xlsx')]
            for future in futures:
                future.result()

    elif has_subfolders and has_xls_files:
        return "La carpeta de entrada contiene subcarpetas y archivos xls"
    else:
        return "La carpeta de entrada está vacía o no contiene archivos xls"

def crear_carpeta_salida(path_salida):
    carpeta_salida = path_salida
    if os.path.exists(carpeta_salida):
        numero_carpeta = 1
        while os.path.exists(carpeta_salida):
            nueva_carpeta = f"{path_salida}_{numero_carpeta}"
            carpeta_salida = os.path.join(os.path.dirname(path_salida), nueva_carpeta)
            numero_carpeta += 1
    os.makedirs(carpeta_salida)
    return carpeta_salida

def procesar_archivos(input_path, path_salida="datos_salida"):
    transformed_df = load_and_transform_excel(input_path)
    if os.path.exists(path_salida):
        existing_df = pd.read_excel(path_salida)
        transformed_df = pd.concat([existing_df, transformed_df], ignore_index=True)
    transformed_df.to_excel(path_salida, index=False)
    print(f"Archivo guardado en: {path_salida}")


procesar_carpeta("datos")
# Para fijar un máximo de threads
# procesar_carpeta("datos", max_threads=5)