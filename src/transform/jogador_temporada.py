import polars as pl
from pathlib import Path
import json

RAW_DIR = Path("data/raw")

# definição das eras — temporada -> nome da era
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


def construir_fato_jogador_temporada() -> pl.DataFrame:
    """
    Lê todos os JSONs de league_dash_player_stats (tipo Base e Advanced)
    e monta a tabela fato principal com métricas derivadas.
    """
    pasta = RAW_DIR / "league_dash_player_stats"
    registros_base = []
    registros_advanced = []

    for arquivo in pasta.glob("*.json"):
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        linhas = dados.get("LeagueDashPlayerStats", [])
        if not linhas:
            continue

        # identifica o tipo pela presença de colunas específicas
        primeira_linha = linhas[0]
        if "TS_PCT" in primeira_linha:
            registros_advanced.extend(linhas)
        else:
            registros_base.extend(linhas)

    # converte pra DataFrame Polars
    df_base = pl.DataFrame(registros_base)
    df_advanced = pl.DataFrame(registros_advanced)

    # junta base + advanced pelo player_id + season
    df = df_base.join(
        df_advanced.select(["PLAYER_ID", "SEASON_ID", "TS_PCT", "USG_PCT", "PIE"]),
        on=["PLAYER_ID", "SEASON_ID"],
        how="left"
    )

    # adiciona era
    df = df.with_columns(
        pl.col("SEASON_ID").map_elements(
            lambda s: ERAS.get(s, "Desconhecida"),
            return_dtype=pl.String
        ).alias("ERA")
    )

    # calcula TS% manualmente onde não veio da API
    df = df.with_columns(
        pl.when(pl.col("TS_PCT").is_null())
        .then(
            pl.col("PTS") / (2 * (pl.col("FGA") + 0.44 * pl.col("FTA")))
        )
        .otherwise(pl.col("TS_PCT"))
        .alias("TS_PCT")
    )

    # calcula eFG%
    df = df.with_columns(
        (
            (pl.col("FGM") + 0.5 * pl.col("FG3M")) / pl.col("FGA")
        ).alias("EFG_PCT")
    )

    return df