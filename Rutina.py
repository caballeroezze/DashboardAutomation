#!/usr/bin/env python
# coding: utf-8

# ## Rutina de ejecuciones automáticas

# In[ ]:

import pyodbc
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
import re
import time
import subprocess


# Parámetros de conexión
servidor = 
base_datos = 
usuario = 
contrasena = 

# Configuración del directorio de logs
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f'rutina_{datetime.now().strftime('%Y%m%d')}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
handler = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=7)
logging.getLogger().addHandler(handler)


# Lista completa de las 38 consultas SQL separadas
consultas = [
    "DROP VIEW IF EXISTS vw_TurnosEmitidos;",
    
"""
create view vw_TurnosEmitidos as
Select 
EmitioFecha, 
EmitioHora, 
DioTurno
from Turnos
WHERE EmitioFecha >= '2025-01-01';
""",

"DROP VIEW IF EXISTS vw_TurnosAtendidos;",

"""
create view vw_TurnosAtendidos as 
select AtendioFecha, AtendioHora, Asistio
from Turnos
WHERE AtendioFecha >= '2025-01-01';
""",

"DROP VIEW IF EXISTS vw_ActividadesMesaEntrada;",

"""
CREATE VIEW vw_ActividadesMesaEntrada AS
SELECT 
    DioTurno AS Usuario,
    EmitioFecha AS Fecha,
    NULL AS AtendioHora,
    EmitioHora
FROM vw_TurnosEmitidos

UNION ALL

SELECT 
    Asistio AS Usuario,
    AtendioFecha AS Fecha,
    AtendioHora,
    NULL AS EmitioHora
FROM vw_TurnosAtendidos;
""",

"DROP VIEW IF EXISTS vw_ActividadesMesaEntradaConNombres;",

"""
CREATE VIEW vw_ActividadesMesaEntradaConNombres AS
SELECT 
    u.NombreCompleto AS Usuario,
    a.Fecha,
    a.AtendioHora,
    a.EmitioHora
FROM vw_ActividadesMesaEntrada a
LEFT JOIN Usuarios u ON u.Codigo = a.Usuario;
""",

"DROP VIEW IF EXISTS vw_ActividadesMesaEntrada2025;",

"""
CREATE VIEW vw_ActividadesMesaEntrada2025 AS
SELECT 
    a.Usuario AS CodigoUsuario,
    u.NombreCompleto AS NombreUsuario,
    a.Fecha,
    a.AtendioHora,
    a.EmitioHora
FROM vw_ActividadesMesaEntrada a
LEFT JOIN Usuarios u ON u.Codigo = a.Usuario;
""",

"DROP VIEW IF EXISTS vw_LegajosManual;",

"""
CREATE VIEW vw_LegajosManual AS
SELECT * FROM (VALUES
    ('Marcela Cuevas', '161'),
    ('LEONARDO CABRERA', '940'),
    ('GUTIERRE ALEXANDER', '877'),
    ('Gabriela Arrua', '552'),
    ('FRANCISCO SANTARONE', '1017'),
    ('Daniela Ibaceta', '474'),
    ('Fabricio Chacon', '697'),
    ('ESQUIVEL CESAR NICOLAS', '935'),
    ('Mauricio Guarnieri', '630'),
    ('Mabel Mereles', '310'),
    ('Matias Montelongo', '456'),
    ('Maximiliano Baez', '749'),
    ('RODRIGUEZ JULIETA SARA', '968'),
    ('SUASNABAR LEANDRO', '966'),
    ('Ana Humacata', '469'),
    ('Marcos Nahuel Bo', '813'),
    ('Rosa Coceres', '284'),
    ('Rocio Porfirio', '637')
) AS v (NombreUsuario, Legajo);
""",

"DROP VIEW IF EXISTS vw_ActividadesMES2025;",

"""
CREATE VIEW vw_ActividadesMES2025 AS
SELECT 
    a.CodigoUsuario,
    l.Legajo,
    a.NombreUsuario,
    a.Fecha,
    a.AtendioHora,
    a.EmitioHora
FROM vw_ActividadesMesaEntrada2025 a
LEFT JOIN vw_LegajosManual l 
    ON UPPER(a.NombreUsuario) = UPPER(l.NombreUsuario);
""",

"DROP VIEW IF EXISTS vw_ActividadesMESFiltrada;",

"""
create view vw_ActividadesMESFiltrada as
select *
from vw_ActividadesMES2025
where Legajo IS NOT NULL;
""",

"DROP VIEW IF EXISTS vw_LegajoResultado;",

"""
CREATE VIEW vw_LegajoResultado AS
SELECT 
    -- Datos del empleado (Leg_data)
    l.leg_numero AS Legajo,
    l.nombre AS Nombre,
    l.apellido AS Apellido,
    l.DNI_numero AS DNI,
    l.cuil AS CUIL,
    l.leg_seccion AS Seccion,
    l.fecha_ingreso AS Fecha_Ingreso,
    l.fecha_egreso AS Fecha_Egreso,

    -- Datos de resultados de fichadas (Resultado)
    r.fecha AS Fecha,
    r.tramo_nombre AS Tramo,
    r.entrada_tramo AS Hora_Entrada,
    r.salida_tramo AS Hora_Salida,
    r.entro_antes AS Entro_Antes,
    r.entro AS Entro,
    r.entro_tarde AS Entro_Tarde,
    r.salio_antes AS Salio_Antes,
    r.salio AS Salio,
    r.salio_tarde AS Salio_Tarde,
    r.entro_tarde_justificada AS Entro_Tarde_Justificada,
    r.salio_antes_justificada AS Salio_Antes_Justificada
FROM TMCEME.dbo.Leg_data l
JOIN TMCEME.dbo.Resultado r ON l.leg_numero = r.leg;
""",

"DROP VIEW IF EXISTS vw_LegajoResultado_2025;",

"""
CREATE VIEW vw_LegajoResultado_2025 AS
SELECT 
    Legajo,
    Nombre,
    Apellido,
    DNI,
    CUIL,
    Seccion,
    Fecha_Ingreso,
	Fecha_Egreso,
    Fecha,
    Tramo,
    Hora_Entrada,
    Hora_Salida,
    Entro_Antes,
    Entro,
    Entro_Tarde,
    Salio_Antes,
    Salio,
    Salio_Tarde,
    Entro_Tarde_Justificada,
    Salio_Antes_Justificada
FROM vw_LegajoResultado
WHERE Fecha >= '2025-01-01';
""",

"DROP VIEW IF EXISTS vw_LegajoResultado_Activos;",

"""
CREATE VIEW vw_LegajoResultado_Activos AS
SELECT 
    Legajo,
    Nombre,
    Apellido,
    DNI,
    CUIL,
    Seccion,
    Fecha_Ingreso,
    Fecha,
    Tramo,
    Hora_Entrada,
    Hora_Salida,
    Entro_Antes,
    Entro,
    Entro_Tarde,
    Salio_Antes,
    Salio,
    Salio_Tarde,
    Entro_Tarde_Justificada,
    Salio_Antes_Justificada
FROM vw_LegajoResultado_2025
WHERE Fecha_Egreso IS NULL;
""",

"DROP VIEW IF EXISTS vw_LegajoResultado_Activos_MES;",

"""
CREATE VIEW vw_LegajoResultado_Activos_MES AS
SELECT 
    Legajo,
    Nombre,
    Apellido,
    DNI,
    CUIL,
    Seccion,
    Fecha_Ingreso,
    Fecha,
    Tramo,
    Hora_Entrada,
    Hora_Salida,
    Entro_Antes,
    Entro,
    Entro_Tarde,
    Salio_Antes,
    Salio,
    Salio_Tarde,
    Entro_Tarde_Justificada,
    Salio_Antes_Justificada
FROM vw_LegajoResultado_Activos
WHERE Seccion = 'MES';
""",

"DROP VIEW IF EXISTS vw_ActividadesCompletas;",

"""
CREATE VIEW vw_ActividadesCompletas AS
SELECT 
    a.CodigoUsuario,
    a.Legajo,
    a.NombreUsuario,
    a.Fecha,
    a.AtendioHora,
    a.EmitioHora,
    
    l.Nombre,
    l.Apellido,
    l.DNI,
    l.CUIL,
    l.Seccion,
    l.Fecha_Ingreso,
    l.Hora_Entrada,
    l.Hora_Salida,
    l.Entro_Antes,
    l.Entro,
    l.Entro_Tarde,
    l.Salio_Antes,
    l.Salio,
    l.Salio_Tarde,
    l.Entro_Tarde_Justificada,
    l.Salio_Antes_Justificada

FROM Gasalud.dbo.vw_ActividadesMESFiltrada a
INNER JOIN Gasalud.dbo.vw_LegajoResultado_Activos_MES l 
    ON a.Legajo = l.Legajo 
    AND a.Fecha = l.Fecha;
""",

"DROP VIEW IF EXISTS vw_ActividadesOrdenadas;",

"""
CREATE VIEW vw_ActividadesOrdenadas AS
SELECT 
    CodigoUsuario,
    Legajo,
    Seccion,
    NombreUsuario,
    Nombre,
    Apellido,
    Fecha,
    Hora_Entrada,
    Entro,
    AtendioHora,
    EmitioHora,
    Hora_Salida,
    Salio,
    Entro_Tarde_Justificada,
    Salio_Antes_Justificada
FROM vw_ActividadesCompletas;
""",
 
"DROP VIEW IF EXISTS vw_PrimerasActividadesPorUsuario;",

"""
CREATE VIEW vw_PrimerasActividadesPorUsuario AS
SELECT 
    CodigoUsuario,
    Legajo,
    Seccion,
    NombreUsuario,
    Nombre,
    Apellido,
    Fecha,
    Hora_Entrada,
    Entro,
    
    -- Primera actividad del día
    MIN(AtendioHora) AS AtendioHora,
    MIN(EmitioHora) AS EmitioHora,
    
    -- Última actividad antes de la hora real de salida (Salio)
    MAX(CASE WHEN AtendioHora <= Salio THEN AtendioHora END) AS UltimaAtendioHora,
    MAX(CASE WHEN EmitioHora <= Salio THEN EmitioHora END) AS UltimaEmitioHora,
    
    Hora_Salida,
    Salio,
    Entro_Tarde_Justificada,
    Salio_Antes_Justificada

FROM vw_ActividadesOrdenadas

GROUP BY 
    CodigoUsuario,
    Legajo,
    Seccion,
    NombreUsuario,
    Nombre,
    Apellido,
    Fecha,
    Hora_Entrada,
    Entro,
    Hora_Salida,
    Salio,
    Entro_Tarde_Justificada,
    Salio_Antes_Justificada;
""",

"DROP VIEW IF EXISTS vw_DiferenciasDeTiempo;",

"""
-- Crear la vista con las diferencias de tiempo y la columna Fecha
CREATE VIEW vw_DiferenciasDeTiempo AS
SELECT 
    Apellido,
    Legajo,
    Fecha,
    Entro,
    AtendioHora,
    EmitioHora,
    Salio,
    UltimaEmitioHora,
    UltimaAtendioHora,

    -- Diferencia de tiempo en formato hh:mm:ss: Entro - AtendioHora
    CASE 
        WHEN Entro IS NOT NULL 
            AND AtendioHora IS NOT NULL 
        THEN CONVERT(VARCHAR, 
            DATEADD(SECOND, 
                DATEDIFF(SECOND, 
                    CAST(CONVERT(VARCHAR(8), Entro, 108) AS TIME), 
                    CAST(CONVERT(VARCHAR(8), AtendioHora, 108) AS TIME)
                ), 
                '00:00:00'
            ), 
            108
        )
        ELSE '00:00:00'
    END AS Diferencia_Atencion1,

    -- Diferencia de tiempo en formato hh:mm:ss: Entro - EmitioHora
    CASE 
        WHEN Entro IS NOT NULL 
            AND EmitioHora IS NOT NULL 
        THEN CONVERT(VARCHAR, 
            DATEADD(SECOND, 
                DATEDIFF(SECOND, 
                    CAST(CONVERT(VARCHAR(8), Entro, 108) AS TIME), 
                    CAST(CONVERT(VARCHAR(8), EmitioHora, 108) AS TIME)
                ), 
                '00:00:00'
            ), 
            108
        )
        ELSE '00:00:00'
    END AS Diferencia_Emision1,

    -- Diferencia de tiempo en formato hh:mm:ss: Salio - UltimaEmitioHora
    CASE 
        WHEN Salio IS NOT NULL 
            AND UltimaEmitioHora IS NOT NULL 
        THEN CONVERT(VARCHAR, 
            DATEADD(SECOND, 
                DATEDIFF(SECOND, 
                    CAST(CONVERT(VARCHAR(8), UltimaEmitioHora, 108) AS TIME), 
                    CAST(CONVERT(VARCHAR(8), Salio, 108) AS TIME)
                ), 
                '00:00:00'
            ), 
            108
        )
        ELSE '00:00:00'
    END AS Diferencia_Ultima_Emision,

    -- Diferencia de tiempo en formato hh:mm:ss: Salio - UltimaAtendioHora
    CASE 
        WHEN Salio IS NOT NULL 
            AND UltimaAtendioHora IS NOT NULL 
        THEN CONVERT(VARCHAR, 
            DATEADD(SECOND, 
                DATEDIFF(SECOND, 
                    CAST(CONVERT(VARCHAR(8), UltimaAtendioHora, 108) AS TIME), 
                    CAST(CONVERT(VARCHAR(8), Salio, 108) AS TIME)
                ), 
                '00:00:00'
            ), 
            108
        )
        ELSE '00:00:00'
    END AS Diferencia_Ultima_Atencion

FROM Gasalud.dbo.vw_PrimerasActividadesPorUsuario;
""",

"DROP VIEW IF EXISTS vw_ActividadesConDiferenciasDeTiempo;",

"""
CREATE VIEW vw_ActividadesConDiferenciasDeTiempo AS
SELECT 
    -- Campos de la vista principal
    p.CodigoUsuario,
    p.Legajo,
    p.Seccion,
    p.NombreUsuario,
    p.Nombre,
    p.Apellido,
    p.Fecha,
    p.Hora_Entrada,
    p.Entro,
    p.AtendioHora,
    p.EmitioHora,
    p.UltimaAtendioHora,
    p.UltimaEmitioHora,
    p.Hora_Salida,
    p.Salio,
    p.Entro_Tarde_Justificada,
    p.Salio_Antes_Justificada,

    -- Columnas adicionales de diferencias de tiempo
    d.Diferencia_Atencion1,
    d.Diferencia_Emision1,
    d.Diferencia_Ultima_Emision,
    d.Diferencia_Ultima_Atencion

FROM Gasalud.dbo.vw_PrimerasActividadesPorUsuario p
LEFT JOIN Gasalud.dbo.vw_DiferenciasDeTiempo d 
    ON p.Legajo = d.Legajo 
    AND p.Fecha = d.Fecha;
""",

"""
DROP VIEW IF EXISTS vw_ActividadesSinDuplicados;
""",

"""
CREATE VIEW vw_ActividadesSinDuplicados AS
WITH CTE AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY Legajo, Fecha, COALESCE(AtendioHora, EmitioHora) 
                              ORDER BY COALESCE(AtendioHora, EmitioHora)) AS rn
    FROM vw_ActividadesOrdenadas
)
SELECT * 
FROM CTE 
WHERE rn = 1;  -- Mantiene solo la primera ocurrencia de cada fichaje
""",

"""
DROP VIEW IF EXISTS vw_ActividadesFusionadas;
""",

"""
CREATE VIEW vw_ActividadesFusionadas AS
WITH CTE AS (
    SELECT *, 
           COALESCE(AtendioHora, EmitioHora) AS HoraActividad,
           ROW_NUMBER() OVER (PARTITION BY Legajo, Fecha, COALESCE(AtendioHora, EmitioHora) 
                              ORDER BY COALESCE(AtendioHora, EmitioHora)) AS rn
    FROM vw_ActividadesOrdenadas
)
SELECT 
    CodigoUsuario,
    Legajo,
    Seccion,
    NombreUsuario,
    Nombre,
    Apellido,
    Fecha,
    HoraActividad,  -- Nueva columna con la fusión de las horas
    Entro_Tarde_Justificada,
    Salio_Antes_Justificada
FROM CTE 
WHERE rn = 1;  -- Mantiene solo la primera ocurrencia de cada fichaje
"""

]


# In[5]:


# Cadena de conexión (utilizando autenticación de SQL Server)
conexion = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={servidor};'
    f'DATABASE={base_datos};'
    f'UID={usuario};'
    f'PWD={contrasena}'
)

# QlikView config
QV_PATH = r"C:\Program Files\QlikView\Qv.exe"
QVW_FILE = r"C:\Users\ecaballero\Documents\archivos\Actividades Usuarios Final.qvw"
QVD_DIR = r"R:\14 - Areas Administrativas\03 - Recursos Humanos\Publica"
EXPECTED_QVDS = ['Diario.qvd', 'Actividades.qvd']

def ejecutar_consultas():
    print(f"\n[INICIO] Ejecutando consultas SQL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        conexion = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={servidor};'
            f'DATABASE={base_datos};'
            f'UID={usuario};'
            f'PWD={contrasena}'
        )
        cursor = conexion.cursor()
        print(f"Conectado a la base de datos: {base_datos}")

        for indice, consulta in enumerate(consultas, start=1):
            consulta = consulta.strip()
            if not consulta:
                continue

            print(f"\n[CONSULTA {indice}] Ejecutando: {consulta[:50]}...")
            try:
                cursor.execute(consulta)
                if consulta.lower().startswith("select"):
                    try:
                        resultados = cursor.fetchall()
                        for fila in resultados:
                            print(f"🔍 Resultado: {fila}")
                    except pyodbc.ProgrammingError as e:
                        print(f"⚠️ Error al obtener resultados: {e}")
                else:
                    conexion.commit()
                    print(f"✅ Consulta ejecutada correctamente")
                time.sleep(1)
            except Exception as e:
                print(f"❌ Error en la consulta {indice}: {e}")
                break

        conexion.close()
    except Exception as e:
        print(f"⚠️ Error de conexión: {e}")

    print(f"[FIN] Ejecución completa - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Ejecutar las consultas al iniciar el script
ejecutar_consultas()

# Ejecutar QlikView

def ejecutar_qlikview():
    logging.info("Ejecutando QlikView...")
    try:
        result = subprocess.run([QV_PATH, '/r', QVW_FILE], capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            logging.info("QlikView ejecutado correctamente")
        else:
            logging.error(f"Error en QlikView: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        logging.error(f"Excepción al ejecutar QlikView: {e}")
        return False

# Validar QVDs

def validar_qvds():
    logging.info("Validando existencia de archivos QVD...")
    errores = []
    for qvd in EXPECTED_QVDS:
        path = os.path.join(QVD_DIR, qvd)
        if not os.path.exists(path):
            errores.append(f"No se encontró el archivo: {qvd}")
        else:
            mod_time = datetime.fromtimestamp(os.path.getmtime(path))
            if (datetime.now() - mod_time).total_seconds() > 3600:
                errores.append(f"Archivo desactualizado: {qvd} modificado hace más de 1h")
    if errores:
        for err in errores:
            logging.warning(err)
        return False
    logging.info("Todos los QVDs validados correctamente")
    return True

# MAIN
if __name__ == "__main__":
    ejecutar_consultas()
    if ejecutar_qlikview():
        if not validar_qvds():
            logging.warning("QVDs generados pero con problemas de validación")
    else:
        logging.error("La ejecución de QlikView falló. No se validarán los QVDs")