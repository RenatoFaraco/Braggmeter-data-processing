import os
import time
from sqlalchemy import create_engine, text

base_path = r"F:\\DOUTORADO_RENATO\\PESQUISAS\\MEUS_TRABALHOS\\AGUA_E_SAL\\data"
db_path   = os.path.join(base_path, 'espectros_dia_07_05.db')
engine    = create_engine(f"sqlite:///{db_path}")

def log_step(message, start_time):
    print(f"{message} — concluído em {time.time() - start_time:.2f} segundos.")

with engine.begin() as conn:
    print("Iniciando atualização do banco de dados...")
    
    t0 = time.time()
    try:
        conn.execute(text("ALTER TABLE spectra RENAME COLUMN dia TO data;"))
        log_step("Coluna 'dia' renomeada para 'data'", t0)
    except Exception as e:
        print("Possivelmente já renomeada ou versão antiga do SQLite:", e)

    t1 = time.time()
    conn.execute(text("""
        UPDATE spectra
        SET data = REPLACE(data, '_', '/') || '/2025';
    """))
    log_step("Atualização dos valores da coluna 'data'", t1)

    t2 = time.time()
    conn.execute(text("""
        UPDATE spectra
        SET rodadas = 'Rodada ' || substr(rodadas, instr(rodadas, '_')+1);
    """))
    log_step("Atualização dos valores da coluna 'rodadas'", t2)

    t3 = time.time()
    conn.execute(text("""
        UPDATE spectra
        SET amostras = CASE amostras
            WHEN 'A_0'   THEN 'Somente água com sal'
            WHEN 'A_20'  THEN '20% solução supersaturada'
            WHEN 'A_33'  THEN '33% solução supersaturada'
            WHEN 'A_50'  THEN '50% solução supersaturada'
            WHEN 'A_100' THEN '100% solução supersaturada'
            ELSE amostras
        END;
    """))
    log_step("Atualização da coluna 'amostras' para nomes descritivos", t3)

    t4 = time.time()
    try:
        conn.execute(text("ALTER TABLE spectra ADD COLUMN brix REAL;"))
        print("Coluna 'brix' adicionada.")
    except Exception as e:
        print("Coluna 'brix' já existe ou erro:", e)

    conn.execute(text("""
        UPDATE spectra
        SET brix = CASE amostras
            WHEN 'Somente água com sal'        THEN 0.0
            WHEN '20% solução supersaturada'   THEN 5.2
            WHEN '33% solução supersaturada'   THEN 8.6
            WHEN '50% solução supersaturada'   THEN 13.0
            WHEN '100% solução supersaturada'  THEN 25.0
            ELSE NULL
        END;
    """))
    log_step("Coluna 'brix' preenchida", t4)

print(f"\n✅ Finalizado em {time.time() - t0:.2f} segundos.")
print("Banco de dados atualizado com sucesso!")