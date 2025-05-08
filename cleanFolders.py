import os

def delete_small_txt_files(root_path, size_threshold_kb=100):
    """
    Percorre todo o conteúdo de root_path (por exemplo '.../data') e:
     1) apaga todos os arquivos .txt < size_threshold_kb KB
     2) remove pastas vazias geradas após essas remoções
    """
    threshold_bytes = size_threshold_kb * 1024

    for dirpath, dirnames, filenames in os.walk(root_path):
        for fname in filenames:
            if not fname.lower().endswith('.txt'):
                continue
            full_path = os.path.join(dirpath, fname)
            try:
                size = os.path.getsize(full_path)
            except OSError as e:
                print(f"[!] Erro ao ler tamanho de {full_path}: {e}")
                continue
            if size < threshold_bytes:
                try:
                    os.remove(full_path)
                    print(f"[DEL] {full_path} ({size} bytes)")
                except OSError as e:
                    print(f"[!] Falha ao remover {full_path}: {e}")

    
    for dirpath, dirnames, filenames in os.walk(root_path, topdown=False):
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
                print(f"[RMDIR] {dirpath} (vazio)")
            except OSError as e:
                print(f"[!] Não foi possível remover pasta vazia {dirpath}: {e}")

if __name__ == '__main__':
    base_data_folder = r"F:\\DOUTORADO_RENATO\\PESQUISAS\\MEUS_TRABALHOS\\AGUA_E_SAL\\data"
    delete_small_txt_files(base_data_folder, size_threshold_kb=100)