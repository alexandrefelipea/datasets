import os
import pandas as pd

def split_csv(input_csv, output_folder, max_size_mb):
    chunk_size = max_size_mb * 1024 * 1024  # Convert max_size_mb to bytes

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open(input_csv, 'r', encoding='utf-8') as f_in:
        header = f_in.readline()  # Read the header line
        current_chunk = 1
        current_chunk_size = 0
        chunk_filename = os.path.join(output_folder, f'part_{current_chunk}.csv')
        f_out = open(chunk_filename, 'w', encoding='utf-8')
        f_out.write(header)

        for line in f_in:
            if current_chunk_size > chunk_size:
                f_out.close()
                current_chunk += 1
                chunk_filename = os.path.join(output_folder, f'part_{current_chunk}.csv')
                f_out = open(chunk_filename, 'w', encoding='utf-8')
                f_out.write(header)
                current_chunk_size = 0

            f_out.write(line)
            current_chunk_size += len(line)

        f_out.close()

def merge_csv(input_folder, output_csv):
    all_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    all_files.sort()
    df_list = []

    for filename in all_files:
        file_path = os.path.join(input_folder, filename)
        df_part = pd.read_csv(file_path)
        df_list.append(df_part)

    df_concat = pd.concat(df_list)
    df_concat.to_csv(output_csv, index=False, encoding='utf-8')

def calculate_percentage(csv_original, csv_unified):
    df_original = pd.read_csv(csv_original)
    df_unified = pd.read_csv(csv_unified)

    total_rows_original = len(df_original)
    total_rows_unified = len(df_unified)

    percentage = (total_rows_unified / total_rows_original) * 100

    return percentage

def process_csv_files(input_dir, max_size_mb):
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_csv = os.path.join(input_dir, filename)
            output_folder = input_csv.replace('.csv', '_parts')
            merged_csv = input_csv.replace('.csv', '_merged.csv')

            split_csv(input_csv, output_folder, max_size_mb)
            merge_csv(output_folder, merged_csv)

            percentage_matched = calculate_percentage(input_csv, merged_csv)
            print(f"A porcentagem de linhas do arquivo {input_csv} que está no arquivo unificado é: {percentage_matched:.2f}%")

    # Apagar os arquivos "_merged.csv" após o processamento
    delete_merged_files(input_dir)
    print("FINALIZADO!")

def delete_merged_files(input_dir):
    for filename in os.listdir(input_dir):
        if filename.endswith('_merged.csv'):
            file_path = os.path.join(input_dir, filename)
            os.remove(file_path)

# Definir o diretório contendo os arquivos CSV
input_dir = "/home/alexandre/Dev/TF2/TF_2_Notebooks_and_Data/00-NumPy-Crash-Course/My/dataset/"

# Definir o tamanho máximo dos chunks em MB
max_size_mb = 24

# Processar todos os arquivos CSV no diretório
process_csv_files(input_dir, max_size_mb)
