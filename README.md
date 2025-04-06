# 🔄 Automatización de Actualización de Tableros QlikView

Este script automatiza la ejecución de vistas SQL y la actualización de un tablero de QlikView (`.qvw`), generando archivos QVD necesarios para visualizar actividades del personal en una organización. También valida que los archivos resultantes estén correctamente generados.

## 📁 Estructura del Proyecto

- `Rutina.py`: Script principal con lógica de ejecución y validaciones.
- `/logs`: Carpeta generada automáticamente donde se almacenan los logs de cada ejecución.

## ⚙️ Requisitos

- Python 3.8+
- SQL Server con acceso a las vistas/tables necesarias
- QlikView Desktop instalado (ruta al ejecutable debe estar actualizada)
- Drivers ODBC: `ODBC Driver 17 for SQL Server`

### 🛠️ Instalación de dependencias

```bash
pip install pyodbc
```

## 🚀 Uso

1. **Configurar parámetros de conexión**:
   Editar `Rutina.py` e ingresar:
   ```python
   servidor = '...'
   base_datos = '...'
   usuario = '...'
   contrasena = '...'
   ```

2. **Ejecutar el script**:

```bash
python Rutina.py
```

Esto:
- Ejecuta las vistas SQL necesarias para el análisis
- Lanza QlikView en modo automático (`/r`) para recargar el documento `.qvw`
- Valida que los archivos QVD fueron generados correctamente

## 🧪 Validaciones

El script verifica:
- Existencia de los QVDs esperados (`Diario.qvd`, `Actividades.qvd`)
- Que estos hayan sido modificados en la última hora

## 📄 Logs

Todos los eventos se almacenan en:
```
/logs/rutina_YYYYMMDD.log
```

Incluye errores de conexión, errores de consultas, y salida de QlikView.

## 🧩 Consultas SQL

El script ejecuta más de 35 vistas SQL que:
- Limpian y normalizan datos
- Cruzan actividades con horarios de fichadas
- Calculan diferencias entre horarios y fichajes
- Generan vistas intermedias para su consumo en QlikView

## 📌 Consideraciones

- QlikView debe poder acceder a la base de datos y generar los QVDs correctamente.
- Si se detectan archivos desactualizados, el script lo advertirá vía log.

## 🧑‍💻 Autor

**Ezequiel Caballero**
