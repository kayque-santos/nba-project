import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

import json
from tqdm import tqdm
from src.api.extractors import extrair_premios_jogador, extrair_carreira_jogador, extrair_info_jogador

RAW_DIR = Path("data/raw")

def carregar_ids_jogadores() -> list:
    """
    Lê os JSONs cacheados de league_dash_player_stats
    e retorna lista única de todos os player_ids encontrados.
    """
    ids = set()
    pasta = RAW_DIR / "league_dash_player_stats"

    if not pasta.exists():
        print("Cache de jogadores não encontrado. Rode o script 01 primeiro.")
        return []

    for arquivo in pasta.glob("*.json"):
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        linhas = dados.get("LeagueDashPlayerStats", [])
        for linha in linhas:
            ids.add(linha["PLAYER_ID"])

    return list(ids)


def extrair_premios_e_carreira():
    ids_jogadores = carregar_ids_jogadores()
    print(f"Total de jogadores únicos encontrados: {len(ids_jogadores)}")
    print("-" * 40)

    erros = []

    for id_jogador in tqdm(ids_jogadores, desc="Prêmios e carreira"):
        # prêmios
        try:
            df_premios = extrair_premios_jogador(id_jogador=id_jogador)
        except Exception as e:
            erros.append(f"premios | {id_jogador} | {e}")

        # carreira
        try:
            df_carreira = extrair_carreira_jogador(id_jogador=id_jogador)
        except Exception as e:
            erros.append(f"carreira | {id_jogador} | {e}")

        # info geral
        try:
            df_info = extrair_info_jogador(id_jogador=id_jogador)
        except Exception as e:
            erros.append(f"info | {id_jogador} | {e}")

    print(f"\nTotal de erros: {len(erros)}")
    for erro in erros:
        print(erro)


if __name__ == "__main__":
    extrair_premios_e_carreira()