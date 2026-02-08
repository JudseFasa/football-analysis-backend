"""
Módulo de predicción de partidos
Basado en modelo de Poisson con factores de ataque/defensa
"""
import pandas as pd


def calcular_promedios_liga(df):
    """
    Calcula promedios de goles de la liga
    
    Args:
        df: DataFrame con partidos filtrados (liga, temporada específica)
        
    Returns:
        dict con promedios de equipos locales y visitantes
    """
    total_partidos = len(df)
    
    if total_partidos == 0:
        raise ValueError("No hay partidos en el DataFrame filtrado")
    
    promedio_local = df["goles_local"].mean()
    promedio_visitante = df["goles_visitante"].mean()
    
    return {
        "C6": promedio_local,      # PROMEDIO EQUIPOS LOCALES
        "E6": promedio_visitante,  # PROMEDIO EQUIPOS VISITANTES
        "total_partidos": total_partidos
    }


def calcular_estadisticas_equipo(df, nombre_equipo, como_local=True):
    """
    Calcula estadísticas de un equipo específico
    
    Args:
        df: DataFrame con partidos
        nombre_equipo: Nombre del equipo
        como_local: True para estadísticas como local, False como visitante
        
    Returns:
        dict con promedio anotado y recibido
    """
    if como_local:
        partidos = df[df["local"] == nombre_equipo]
        goles_anotados = partidos["goles_local"]
        goles_recibidos = partidos["goles_visitante"]
    else:
        partidos = df[df["visitante"] == nombre_equipo]
        goles_anotados = partidos["goles_visitante"]
        goles_recibidos = partidos["goles_local"]
    
    if len(partidos) == 0:
        raise ValueError(f"No se encontraron partidos para {nombre_equipo}")
    
    return {
        "promedio_anotado": goles_anotados.mean(),
        "promedio_recibido": goles_recibidos.mean(),
        "partidos_jugados": len(partidos)
    }


def calcular_factores_ataque_defensa(equipo_stats, promedio_liga):
    """
    Calcula factores de ataque y defensa relativos a la liga
    
    Args:
        equipo_stats: dict con promedio_anotado y promedio_recibido
        promedio_liga: promedio de goles de la liga
        
    Returns:
        dict con factor_ataque y factor_defensa
    """
    factor_ataque = equipo_stats["promedio_anotado"] / promedio_liga
    factor_defensa = equipo_stats["promedio_recibido"] / promedio_liga
    
    return {
        "factor_ataque": factor_ataque,
        "factor_defensa": factor_defensa
    }


def predecir_partido(df, equipo_local, equipo_visitante, umbral=2.5):
    """
    Predice el resultado de un partido usando modelo de Poisson
    
    Args:
        df: DataFrame con partidos (debe estar filtrado por liga/temporada)
        equipo_local: Nombre del equipo local
        equipo_visitante: Nombre del equipo visitante
        umbral: Umbral de goles para recomendación (default 2.5)
        
    Returns:
        dict con predicción completa
    """
    # 1. Calcular promedios de la liga
    liga_stats = calcular_promedios_liga(df)
    C6 = liga_stats["C6"]  # Promedio local
    E6 = liga_stats["E6"]  # Promedio visitante
    
    # 2. Estadísticas del equipo local
    local_stats = calcular_estadisticas_equipo(df, equipo_local, como_local=True)
    G6 = local_stats["promedio_anotado"]   # Promedio anotado local
    I6 = local_stats["promedio_recibido"]  # Promedio recibido local
    
    # 3. Estadísticas del equipo visitante
    visitante_stats = calcular_estadisticas_equipo(df, equipo_visitante, como_local=False)
    C11 = visitante_stats["promedio_anotado"]   # Promedio anotado visitante
    E11 = visitante_stats["promedio_recibido"]  # Promedio recibido visitante
    
    # 4. Calcular factores (normalización por liga)
    L8 = G6 / C6       # Ataque local relativo
    L10 = E11 / C6     # Defensa visitante relativa (permite goles al local)
    
    L16 = C11 / E6     # Ataque visitante relativo
    L17 = I6 / E6      # Defensa local relativa (permite goles al visitante)
    
    # 5. Goles esperados (λ implícitos)
    L12 = L8 * L10 * C6   # Goles esperados local
    L19 = L16 * L17 * E6  # Goles esperados visitante
    
    # 6. Goles totales esperados
    L36 = L12 + L19
    
    # 7. Recomendación
    umbral_recomendacion = umbral - 0.1  # 2.4 para umbral de 2.5
    recomendacion = f"MÁS de {umbral} GOLES" if L36 > umbral_recomendacion else f"MENOS de {umbral} GOLES"
    
    # 8. Resultado completo
    return {
        # Datos de entrada
        "equipo_local": equipo_local,
        "equipo_visitante": equipo_visitante,
        "umbral": umbral,
        
        # Promedios de liga
        "liga_promedio_local": round(C6, 2),
        "liga_promedio_visitante": round(E6, 2),
        "partidos_liga": liga_stats["total_partidos"],
        
        # Estadísticas equipos
        "local_anotado": round(G6, 2),
        "local_recibido": round(I6, 2),
        "local_partidos": local_stats["partidos_jugados"],
        
        "visitante_anotado": round(C11, 2),
        "visitante_recibido": round(E11, 2),
        "visitante_partidos": visitante_stats["partidos_jugados"],
        
        # Factores relativos
        "local_factor_ataque": round(L8, 3),
        "local_factor_defensa": round(L17, 3),
        "visitante_factor_ataque": round(L16, 3),
        "visitante_factor_defensa": round(L10, 3),
        
        # Predicción
        "goles_esperados_local": round(L12, 2),
        "goles_esperados_visitante": round(L19, 2),
        "goles_esperados_total": round(L36, 2),
        
        # Recomendación final
        "recomendacion": recomendacion,
        "confianza": "ALTA" if abs(L36 - umbral) > 0.5 else "MEDIA" if abs(L36 - umbral) > 0.3 else "BAJA"
    }


def imprimir_prediccion(prediccion):
    """
    Imprime la predicción de forma visual y clara
    
    Args:
        prediccion: dict retornado por predecir_partido()
    """
    print("\n" + "=" * 60)
    print("PREDICCIÓN DE PARTIDO".center(60))
    print("=" * 60)
    
    print(f"\n🏠 {prediccion['equipo_local']} vs {prediccion['equipo_visitante']} ✈️")
    print(f"Umbral: {prediccion['umbral']} goles\n")
    
    print("─" * 60)
    print("DATOS DE LA LIGA")
    print("─" * 60)
    print(f"Promedio goles local    : {prediccion['liga_promedio_local']}")
    print(f"Promedio goles visitante: {prediccion['liga_promedio_visitante']}")
    print(f"Total partidos analizados: {prediccion['partidos_liga']}")
    
    print("\n" + "─" * 60)
    print(f"EQUIPO LOCAL: {prediccion['equipo_local']}")
    print("─" * 60)
    print(f"Promedio anotado  : {prediccion['local_anotado']} (factor: {prediccion['local_factor_ataque']})")
    print(f"Promedio recibido : {prediccion['local_recibido']} (factor: {prediccion['local_factor_defensa']})")
    print(f"Partidos jugados  : {prediccion['local_partidos']}")
    
    print("\n" + "─" * 60)
    print(f"EQUIPO VISITANTE: {prediccion['equipo_visitante']}")
    print("─" * 60)
    print(f"Promedio anotado  : {prediccion['visitante_anotado']} (factor: {prediccion['visitante_factor_ataque']})")
    print(f"Promedio recibido : {prediccion['visitante_recibido']} (factor: {prediccion['visitante_factor_defensa']})")
    print(f"Partidos jugados  : {prediccion['visitante_partidos']}")
    
    print("\n" + "=" * 60)
    print("PREDICCIÓN".center(60))
    print("=" * 60)
    print(f"Goles esperados {prediccion['equipo_local']:<20}: {prediccion['goles_esperados_local']}")
    print(f"Goles esperados {prediccion['equipo_visitante']:<20}: {prediccion['goles_esperados_visitante']}")
    print(f"\n{'TOTAL GOLES ESPERADOS':<35}: {prediccion['goles_esperados_total']}")
    
    print("\n" + "─" * 60)
    print(f"RECOMENDACIÓN: {prediccion['recomendacion']}")
    print(f"CONFIANZA: {prediccion['confianza']}")
    print("=" * 60 + "\n")