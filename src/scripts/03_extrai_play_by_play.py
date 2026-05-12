import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from tqdm import tqdm
from src.api.extractors import extrair_play_by_play, extrair_box_score

# Game 7s e jogos decisivos históricos — game_ids oficiais da NBA
JOGOS_DECISIVOS = {
    # Finals
    "1998_Finals_G6_Jordan_ultimo_titulo":  "0041970406",
    "2016_Finals_G7_LeBron_block_Kyrie":    "0042015406",
    "2013_Finals_G6_Ray_Allen":             "0042012406",
    "2010_Finals_G7_Kobe_vs_Celtics":       "0042090407",
    "2005_Finals_G7_Spurs_vs_Pistons":      "0042040407",

    # Semifinais e conferências
    "2019_WCF_G6_Lillard_buzzer_OKC":       "0041800224",
    "2007_ECF_G5_LeBron_48pts_Pistons":     "0042060225",

    # Performances históricas
    "2006_Kobe_81pts":                      "0020500490",
    "2015_Klay_37pts_1quarto":              "0021400502",
    "2023_Finals_G4_Jokic":                 "0042200404",
}


def extrair_jogos_decisivos():
    print(f"Total de jogos: {len(JOGOS_DECISIVOS)}")
    print("-" * 40)

    erros = []

    for nome, game_id in tqdm(JOGOS_DECISIVOS.items(), desc="Jogos"):
        try:
            df_pbp = extrair_play_by_play(game_id=game_id)
            df_box = extrair_box_score(game_id=game_id)
            print(f"  {nome} | {df_pbp.shape[0]} jogadas | {df_box.shape[0]} jogadores")
        except Exception as e:
            erros.append(f"{nome} | {game_id} | {e}")

    print(f"\nTotal de erros: {len(erros)}")
    for erro in erros:
        print(erro)


if __name__ == "__main__":
    extrair_jogos_decisivos()