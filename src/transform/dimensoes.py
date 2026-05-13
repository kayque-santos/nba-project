import polars as pl
from pathlib import Path
import json

RAW_DIR = Path("data/raw")

ERAS = {
    "1996-97": "Late Jordan",
    "1997-98": "Late Jordan",
    "1998-99": "Late Jordan",
    "1999-00": "Era Shaq/Duncan",
    "2000-01": "Era Shaq/Duncan",
    "2001-02": "Era Shaq/Duncan",
    "2002-03": "Era Shaq/Duncan",
    "2003-04": "Era Shaq/Duncan",
    "2004-05": "Era Kobe/LeBron",
    "2005-06": "Era Kobe/LeBron",
    "2006-07": "Era Kobe/LeBron",
    "2007-08": "Era Kobe/LeBron",
    "2008-09": "Era Kobe/LeBron",
    "2009-10": "Era Kobe/LeBron",
    "2010-11": "Era Kobe/LeBron",
    "2011-12": "Pre-Warriors",
    "2012-13": "Pre-Warriors",
    "2013-14": "Pre-Warriors",
    "2014-15": "Pre-Warriors",
    "2015-16": "Pre-Warriors",
    "2016-17": "Era Warriors",
    "2017-18": "Era Warriors",
    "2018-19": "Era Warriors",
    "2019-20": "Era Warriors",
    "2020-21": "Era Moderna",
    "2021-22": "Era Moderna",
    "2022-23": "Era Moderna",
    "2023-24": "Era Moderna",
    "2024-25": "Era Moderna",
}


def construir_dim_jogador() -> pl.DataFrame:
    """
    Lê os JSONs de info_jogador e monta a dimensão de jogadores
    com metadados: altura, peso, posição, país, draft.
    """
    pasta = RAW_DIR / "info_jogador"
    registros = []

    if not pasta.exists():
        print("Cache de info_jogador não encontrado. Rode o script 04 primeiro.")
        return pl.DataFrame()

    for arquivo in pasta.glob("*.json"):
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        linhas = dados.get("CommonPlayerInfo", [])
        if linhas:
            registros.append(linhas[0])  # 1 registro por jogador

    df = pl.DataFrame(registros)

    # seleciona só as colunas relevantes
    colunas = [
        "PERSON_ID", "DISPLAY_FIRST_LAST", "POSITION",
        "HEIGHT", "WEIGHT", "COUNTRY", "DRAFT_YEAR",
        "DRAFT_ROUND", "DRAFT_NUMBER", "BIRTHDATE"
    ]
    # filtra só colunas que existem no df
    colunas_existentes = [c for c in colunas if c in df.columns]

    return df.select(colunas_existentes).rename({"PERSON_ID": "PLAYER_ID"})


def construir_dim_time() -> pl.DataFrame:
    """
    Lê os JSONs de league_dash_team_stats e monta a dimensão de times.
    """
    pasta = RAW_DIR / "league_dash_team_stats"
    registros = []

    for arquivo in pasta.glob("*.json"):
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        linhas = dados.get("LeagueDashTeamStats", [])
        for linha in linhas:
            registros.append({
                "TEAM_ID": linha["TEAM_ID"],
                "TEAM_NAME": linha["TEAM_NAME"],
                "TEAM_ABBREVIATION": linha.get("TEAM_ABBREVIATION", ""),
            })

    df = pl.DataFrame(registros)

    # remove duplicatas — mesmo time aparece em várias temporadas
    return df.unique(subset=["TEAM_ID"])


def construir_dim_temporada() -> pl.DataFrame:
    """
    Monta a dimensão de temporadas com ano inicio, ano fim e era.
    """
    temporadas = []
    for ano in range(1996, 2025):
        season_id = f"{ano}-{str(ano + 1)[2:]}"
        temporadas.append({
            "SEASON_ID": season_id,
            "ANO_INICIO": ano,
            "ANO_FIM": ano + 1,
            "ERA": ERAS.get(season_id, "Desconhecida"),
        })

    return pl.DataFrame(temporadas)


def construir_dim_era() -> pl.DataFrame:
    """
    Monta a dimensão de eras com ordem cronológica.
    """
    eras = [
        {"ERA_ID": 1, "ERA_NOME": "Late Jordan",     "TEMPORADA_INICIO": "1996-97", "TEMPORADA_FIM": "1998-99"},
        {"ERA_ID": 2, "ERA_NOME": "Era Shaq/Duncan", "TEMPORADA_INICIO": "1999-00", "TEMPORADA_FIM": "2003-04"},
        {"ERA_ID": 3, "ERA_NOME": "Era Kobe/LeBron", "TEMPORADA_INICIO": "2004-05", "TEMPORADA_FIM": "2010-11"},
        {"ERA_ID": 4, "ERA_NOME": "Pre-Warriors",    "TEMPORADA_INICIO": "2011-12", "TEMPORADA_FIM": "2015-16"},
        {"ERA_ID": 5, "ERA_NOME": "Era Warriors",    "TEMPORADA_INICIO": "2016-17", "TEMPORADA_FIM": "2019-20"},
        {"ERA_ID": 6, "ERA_NOME": "Era Moderna",     "TEMPORADA_INICIO": "2020-21", "TEMPORADA_FIM": "2024-25"},
    ]

    return pl.DataFrame(eras)