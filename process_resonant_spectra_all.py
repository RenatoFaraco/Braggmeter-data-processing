import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import process_spectra.funcs as funcs


def process_and_save(day_path, output_db_path):
    all_dfs = []
    all_spectra_lpg_1 = []
    all_spectra_lpg_2 = []
    day = os.path.basename(day_path)

    for rodadas in os.listdir(day_path):
        rodadas_path = os.path.join(day_path, rodadas)
        if not os.path.isdir(rodadas_path):
            continue

        for amostra in os.listdir(rodadas_path):
            amostra_path = os.path.join(rodadas_path, amostra)
            if not os.path.isdir(amostra_path):
                continue

            i = 0
            rp = 3
            dwl = 2
            spec_lpg_1 = []
            spec_lpg_2 = []
            filtrado_1 = []
            filtrado_2 = []

            for fname in os.listdir(amostra_path):
                if not fname.lower().endswith('.txt'):
                    continue
                full_file = os.path.join(amostra_path, fname)
                try:
                    df = pd.read_csv(full_file, sep='\t', decimal=',', header=None)
                    df.columns = [
                        'wavelength',
                        'intensity_measurement_LPG',
                        'intensity_reference_LPG',
                        'noise_1',
                        'noise_2'
                    ]
                    df.drop(columns=['noise_1', 'noise_2'], inplace=True)
                    df['rodadas'] = rodadas
                    df['amostras'] = amostra
                    df['dia'] = day

                    df1 = df[['wavelength', 'intensity_measurement_LPG']]
                    df2 = df[['wavelength', 'intensity_reference_LPG']]

                    data_lpg_1 = df1.to_numpy()
                    mask1 = (data_lpg_1[:, 0] > 1500) & (data_lpg_1[:, 0] < 1600)
                    if np.any(mask1):
                        filtered_data_lpg_1 = data_lpg_1[mask1]
                        spec_lpg_1.append(filtered_data_lpg_1)
                        filtrado_1.append(funcs.filter_spectrum(spec_lpg_1[i], None, 25, 2, quiet=True))
                        spec_lpg_1[i] = filtrado_1[i][0]
                    all_spectra_lpg_1.append(spec_lpg_1[i])

                    data_lpg_2 = df2.to_numpy()
                    mask2 = (data_lpg_2[:, 0] > 1500) & (data_lpg_2[:, 0] < 1600)
                    if np.any(mask2):
                        filtered_data_lpg_2 = data_lpg_2[mask2]
                        spec_lpg_2.append(filtered_data_lpg_2)
                        filtrado_2.append(funcs.filter_spectrum(spec_lpg_2[i], None, 25, 2, quiet=True))
                        spec_lpg_2[i] = filtrado_2[i][0]
                    all_spectra_lpg_2.append(spec_lpg_2[i])

                    all_dfs.append(df)

                except Exception as e:
                    print(f"[!] Falha ao ler/processar {full_file}: {e}")

    results = []
    for i, df in enumerate(all_dfs):
        info = {}
        all_spectra_lpg_1[i], info = funcs.get_approximate_valley(
            all_spectra_lpg_1[i], info, prominence=1, resolution_proximity=rp, p0=None, dwl=dwl
        )
        wl1 = info.get('resonant_wl', None)

        info = {}
        all_spectra_lpg_2[i], info = funcs.get_approximate_valley(
            all_spectra_lpg_2[i], info, prominence=1, resolution_proximity=rp, p0=None, dwl=dwl
        )
        wl2 = info.get('resonant_wl', None)

        metadata = df.iloc[1:].copy()

        result = {
            'dia': metadata['dia'].iloc[0],
            'rodadas': metadata['rodadas'].iloc[0],
            'amostras': metadata['amostras'].iloc[0],
            'wavelength_LPG_measurement': wl1,
            'wavelength_LPG_reference': wl2
        }

        results.append(result)

    final_df = pd.DataFrame(results)
    column_order = ['wavelength_LPG_measurement', 'wavelength_LPG_reference', 'rodadas', 'amostras','dia']
    final_df = final_df[column_order]

    return final_df 


def main():
    base_path = r"F:\\DOUTORADO_RENATO\\PESQUISAS\\MEUS_TRABALHOS\\AGUA_E_SAL\\data"
    output_db = os.path.join(base_path, 'todos_comprimentos_de_onda.db')
    engine = create_engine(f"sqlite:///{output_db}")
    
    for folder in os.listdir(base_path):
        day_path = os.path.join(base_path, folder)
        if not os.path.isdir(day_path):
            continue

        print(f"Processando dia: {folder}")
        df_day = process_and_save(day_path, output_db)
        if df_day is None:
            print(f"  Sem dados em {folder}, pulando.")
            continue
        
        df_day.to_sql(name='resonance_summary', 
                      con=engine, 
                      if_exists='append', 
                      index=False)
        print(f"  Inseridos {len(df_day)} linhas para {folder}.")

    print(f"Concluído. Banco SQLite em: {output_db}")

    #engine = create_engine(f"sqlite:///{output_db_path}")
    #final_df.to_sql(name='resonance_summary', con=engine, if_exists='replace', index=False)
    #print(f"Salvo com sucesso: {len(final_df)} linhas em {output_db_path}")

if __name__ == '__main__':
    main()
