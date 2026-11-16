@echo off
REM ============================================================================
REM Equivalente de crear_topico.sh para Windows (CMD o PowerShell).
REM Requisito: Kafka levantado con  docker compose -f kafka\docker_compose.yml up -d
REM
REM Uso:
REM   kafka\crear_topico.bat           -> crea los topicos si no existen y los lista
REM   kafka\crear_topico.bat --reset   -> BORRA y recrea los topicos (offsets en 0)
REM ============================================================================

set CONTENEDOR=kafka
set BROKER=localhost:9092
set KT=/opt/kafka/bin/kafka-topics.sh
set TOPICOS=estacion-lascondes estacion-lascondes-demo

docker inspect -f "{{.State.Running}}" %CONTENEDOR% 2>nul | findstr /i "true" >nul
if errorlevel 1 (
  echo [ERROR] El contenedor "%CONTENEDOR%" no esta corriendo.
  echo         Ejecuta: docker compose -f kafka\docker_compose.yml up -d
  exit /b 1
)

if "%1"=="--reset" (
  for %%t in (%TOPICOS%) do (
    echo [*] Eliminando topico %%t si existe...
    docker exec %CONTENEDOR% %KT% --delete --if-exists --topic %%t --bootstrap-server %BROKER%
  )
  timeout /t 3 /nobreak >nul
)

for %%t in (%TOPICOS%) do (
  echo [*] Creando topico %%t...
  docker exec %CONTENEDOR% %KT% --create --if-not-exists --topic %%t --bootstrap-server %BROKER% --partitions 1 --replication-factor 1
)

echo.
echo === Topicos disponibles en %BROKER% ===
docker exec %CONTENEDOR% %KT% --list --bootstrap-server %BROKER%

echo.
echo === Detalle del topico principal ===
docker exec %CONTENEDOR% %KT% --describe --topic estacion-lascondes --bootstrap-server %BROKER%
