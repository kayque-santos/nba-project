import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from tqdm import tqdm
from nba_api.stats.static import players
from src.api.extractors import extrair_mapa_arremessos

TEMPORADAS = [f"{ano}-{str(ano + 1)[2:]}" for ano in range(1996, 2025)]

def extrair_todos_shot_charts():
    jogadores_ativos = players.get_active_players()
    print(f"Total de jogadores: {len(jogadores_ativos)}")
    print(f"Total de temporadas: {len(TEMPORADAS)}")
    print("-" * 40)

    erros = []

    for jogador in tqdm(jogadores_ativos, desc="Jogadores"):
        for temporada in TEMPORADAS:
            try:
                df = extrair_mapa_arremessos(
                    id_jogador=jogador["id"],
                    temporada=temporada
                )
                if df.shape[0] > 0:
                    print(f"  {jogador['full_name']} | {temporada} | {df.shape[0]} arremessos")
            except Exception as e:
                erros.append(f"{jogador['full_name']} | {temporada} | {e}")

    print("\n--- ERROS ---")
    for erro in erros:
        print(erro)
    print(f"\nTotal de erros: {len(erros)}")

if __name__ == "__main__":
    extrair_todos_shot_charts()