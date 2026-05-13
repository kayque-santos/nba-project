import polars as pl
from pathlib import Path
import json

RAW_DIR = Path("data/raw")

TIPOS_EVENTO = {
    1: "Arremesso convertido",
    2: "Arremesso errado",
    3: "Lance livre",
    4: "Rebote",
    5: "Turnover",
    6: "Falta",
    7: "Violação",
    8: "Substituição",
    9: "Timeout",
    10: "Bola ao alto",
    11: "Ejeção",
    12: "Início de período",
    13: "Fim de período",
    18: "Replay",
}


def construir_fato_play_by_play() -> pl.DataFrame:
    pasta = RAW_DIR / "play_by_play"
    registros = []

    for arquivo in pasta.glob("*.json"):
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        linhas = dados.get("PlayByPlay", [])
        if not linhas:
            continue
        registros.extend(linhas)

    df = pl.DataFrame(registros)

    df = df.select([
        "GAME_ID",
        "EVENTNUM",
        "PERIOD",
        "PCTIMESTRING",
        "EVENTMSGTYPE",
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "NEUTRALDESCRIPTION",
        "SCORE",
        "SCOREMARGIN",
        "PLAYER1_ID",
        "PLAYER1_NAME",
        "PLAYER1_TEAM_ID",
    ])

    df = df.with_columns(
        (pl.col("GAME_ID").cast(pl.String) + "_" + pl.col("EVENTNUM").cast(pl.String))
        .alias("EVENTO_ID")
    )

    df = df.with_columns(
        pl.col("EVENTMSGTYPE").map_elements(
            lambda t: TIPOS_EVENTO.get(t, "Outro"),
            return_dtype=pl.String
        ).alias("TIPO_EVENTO")
    )

    df = df.with_columns(
        pl.coalesce([
            pl.col("HOMEDESCRIPTION"),
            pl.col("VISITORDESCRIPTION"),
            pl.col("NEUTRALDESCRIPTION"),
        ]).alias("DESCRICAO")
    )

    df = df.with_columns(
        pl.col("SCORE").str.split(" - ").list.get(0, null_on_oob=True).cast(pl.Int32, strict=False).alias("PLACAR_VISITANTE"),
        pl.col("SCORE").str.split(" - ").list.get(1, null_on_oob=True).cast(pl.Int32, strict=False).alias("PLACAR_CASA"),
    )

    df = df.drop([
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "NEUTRALDESCRIPTION",
        "EVENTMSGTYPE",
        "SCORE",
    ])

    return df