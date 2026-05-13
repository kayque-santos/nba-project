import polars as pl
from pathlib import Path
import json

RAW_DIR = Path("data/raw")

ZONAS_NORMALIZADAS = {
    "Left Corner 3": "Corner 3",
    "Right Corner 3": "Corner 3",
    "In The Paint (Non-RA)": "Paint (Non-RA)",
}


def construir_fato_arremessos() -> pl.DataFrame:
    pasta = RAW_DIR / "shot_charts"
    registros = []

    for arquivo in pasta.glob("*.json"):
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        linhas = dados.get("Shot_Chart_Detail", [])
        if not linhas:
            continue
        registros.extend(linhas)

    df = pl.DataFrame(registros)

    df = df.with_columns(
        pl.col("SHOT_ZONE_BASIC").map_elements(
            lambda z: ZONAS_NORMALIZADAS.get(z, z),
            return_dtype=pl.String
        ).alias("ZONA")
    )

    df = df.with_columns(
        (pl.col("GAME_ID").cast(pl.String) + "_" + pl.col("GAME_EVENT_ID").cast(pl.String))
        .alias("ARREMESSO_ID")
    )

    df = df.with_columns(
        pl.when(pl.col("SHOT_TYPE") == "3PT Field Goal")
        .then(3)
        .otherwise(2)
        .cast(pl.Int8)
        .alias("PONTOS_POTENCIAIS")
    )

    df = df.with_columns(
        (pl.col("SHOT_MADE_FLAG") * pl.col("PONTOS_POTENCIAIS"))
        .cast(pl.Int8)
        .alias("PONTOS_MARCADOS")
    )

    return df


def construir_fato_arremessos_zonas(df_arremessos: pl.DataFrame) -> pl.DataFrame:
    df = df_arremessos.group_by(
        ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "SEASON_ID", "ZONA"]
    ).agg(
        pl.len().alias("TENTATIVAS"),
        pl.col("SHOT_MADE_FLAG").sum().alias("ACERTOS"),
        pl.col("PONTOS_MARCADOS").sum().alias("PONTOS"),
        pl.col("SHOT_DISTANCE").mean().alias("DISTANCIA_MEDIA"),
        pl.col("PONTOS_POTENCIAIS").first().alias("PONTOS_POTENCIAIS"),
    )

    df = df.with_columns(
        (pl.col("ACERTOS") / pl.col("TENTATIVAS")).alias("PCT_ACERTO")
    )

    df = df.with_columns(
        pl.when(pl.col("PONTOS_POTENCIAIS") == 3)
        .then((pl.col("ACERTOS") * 1.5) / pl.col("TENTATIVAS"))
        .otherwise(pl.col("ACERTOS") / pl.col("TENTATIVAS"))
        .alias("EFG_ZONA")
    )

    df = df.with_columns(
        pl.col("TENTATIVAS").sum().over(["PLAYER_ID", "SEASON_ID"]).alias("TOTAL_JOGADOR")
    ).with_columns(
        (pl.col("TENTATIVAS") / pl.col("TOTAL_JOGADOR")).alias("PCT_TENTATIVAS_JOGADOR")
    ).drop("TOTAL_JOGADOR")

    return df


def construir_fato_arremessos_liga(df_arremessos: pl.DataFrame) -> pl.DataFrame:
    df = df_arremessos.group_by(["SEASON_ID", "ZONA"]).agg(
        pl.len().alias("TENTATIVAS"),
        pl.col("SHOT_MADE_FLAG").sum().alias("ACERTOS"),
        pl.col("PONTOS_MARCADOS").sum().alias("PONTOS"),
    )

    df = df.with_columns(
        (pl.col("ACERTOS") / pl.col("TENTATIVAS")).alias("PCT_ACERTO")
    )

    df = df.with_columns(
        pl.col("TENTATIVAS").sum().over("SEASON_ID").alias("TOTAL_LIGA")
    ).with_columns(
        (pl.col("TENTATIVAS") / pl.col("TOTAL_LIGA")).alias("PCT_TENTATIVAS_LIGA")
    ).drop("TOTAL_LIGA")

    return df