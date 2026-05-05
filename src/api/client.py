import hashlib
import json
import time
from pathlib import Path

RAW_DIR = Path("data/raw")

def chamar_com_cache(nome_endpoint: str, parametros: dict, fetch_fn):
    """
    Checa cache local antes de chamar a API.
    Se já existe o JSON salvo, retorna do disco.
    Se não existe, chama a API, salva e retorna.
    """
    key = hashlib.md5(json.dumps(parametros, sort_keys=True).encode()).hexdigest()
    path = RAW_DIR/nome_endpoint/f"{key}.json"

    if path.exists():
        print(f"[CACHE] {nome_endpoint} - {key}")
        return json.loads(path.read_text(encoding="utf-8"))
    
    print(f"[API] {nome_endpoint} - chamando...")
    data = fetch_fn()

    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False),encoding="utf-8")

    time.sleep(0.7) # rate limit defensivo

    return data