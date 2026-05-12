import polars as pl

from nba_api.stats.endpoints import (
    leaguedashplayerstats,
    leaguedashteamstats,
    shotchartdetail,
    playercareerstats,
    commonplayerinfo,
    playergamelog,
    playbyplayv3,
    boxscoreadvancedv3,
    playerawards,
)
from src.api.client import chamar_com_cache
from src.api.client import chamar_com_cache


def extrair_stats_jogadores(temporada: str, tipo_metrica: str = "Base") -> pl.DataFrame:
    parametros = {"temporada": temporada, "tipo_metrica": tipo_metrica}

    dados = chamar_com_cache(
        nome_endpoint="league_dash_player_stats",
        parametros=parametros,
        fetch_fn=lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            season=temporada,
            measure_type_detailed_defense=tipo_metrica,
            season_type_all_star="Regular Season"
        ).get_normalized_dict()
    )

    linhas = dados["LeagueDashPlayerStats"]
    return pl.DataFrame(linhas)


def extrair_stats_times(temporada: str) -> pl.DataFrame:
    parametros = {"temporada": temporada}

    dados = chamar_com_cache(
        nome_endpoint="league_dash_team_stats",
        parametros=parametros,
        fetch_fn=lambda: leaguedashteamstats.LeagueDashTeamStats(
            season=temporada,
            season_type_all_star="Regular Season"
        ).get_normalized_dict()
    )

    linhas = dados["LeagueDashTeamStats"]
    return pl.DataFrame(linhas)


def extrair_mapa_arremessos(id_jogador: int, temporada: str) -> pl.DataFrame:
    parametros = {"id_jogador": id_jogador, "temporada": temporada}

    dados = chamar_com_cache(
        nome_endpoint="shot_charts",
        parametros=parametros,
        fetch_fn=lambda: shotchartdetail.ShotChartDetail(
            team_id=0,
            player_id=id_jogador,
            season_nullable=temporada,
            season_type_all_star="Regular Season",
            context_measure_simple="FGA"
        ).get_normalized_dict()
    )

    linhas = dados["Shot_Chart_Detail"]
    return pl.DataFrame(linhas)

def extrair_play_by_play(game_id: str) -> pl.DataFrame:
    parametros = {"game_id": game_id}

    dados = chamar_com_cache(
        nome_endpoint="play_by_play",
        parametros=parametros,
        fetch_fn=lambda: playbyplayv3.PlayByPlayV3(
            game_id=game_id
        ).get_normalized_dict()
    )

    linhas = dados["PlayByPlay"]
    return pl.DataFrame(linhas)


def extrair_box_score(game_id: str) -> pl.DataFrame:
    parametros = {"game_id": game_id}

    dados = chamar_com_cache(
        nome_endpoint="box_score",
        parametros=parametros,
        fetch_fn=lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(
            game_id=game_id
        ).get_normalized_dict()
    )

    linhas = dados["PlayerStats"]
    return pl.DataFrame(linhas)

def extrair_premios_jogador(id_jogador: int) -> pl.DataFrame:
    parametros = {"id_jogador": id_jogador}

    dados = chamar_com_cache(
        nome_endpoint="premios_jogador",
        parametros=parametros,
        fetch_fn=lambda: playerawards.PlayerAwards(
            player_id=id_jogador
        ).get_normalized_dict()
    )

    linhas = dados["PlayerAwards"]
    return pl.DataFrame(linhas)


def extrair_carreira_jogador(id_jogador: int) -> pl.DataFrame:
    parametros = {"id_jogador": id_jogador}

    dados = chamar_com_cache(
        nome_endpoint="carreira_jogador",
        parametros=parametros,
        fetch_fn=lambda: playercareerstats.PlayerCareerStats(
            player_id=id_jogador
        ).get_normalized_dict()
    )

    linhas = dados["SeasonTotalsRegularSeason"]
    return pl.DataFrame(linhas)


def extrair_info_jogador(id_jogador: int) -> pl.DataFrame:
    parametros = {"id_jogador": id_jogador}

    dados = chamar_com_cache(
        nome_endpoint="info_jogador",
        parametros=parametros,
        fetch_fn=lambda: commonplayerinfo.CommonPlayerInfo(
            player_id=id_jogador
        ).get_normalized_dict()
    )

    linhas = dados["CommonPlayerInfo"]
    return pl.DataFrame(linhas)