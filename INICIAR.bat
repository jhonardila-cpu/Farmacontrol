@echo off
chcp 65001 >nul
title Drogueria JYM 2

echo.
echo  Drogueria JYM 2 — Iniciando servidor...
echo.

node -v >nul 2>&1
if errorlevel 1 (
  echo  [ERROR] Node.js no encontrado. Descargalo de: https://nodejs.org
  pause & exit /b 1
)

if not exist "node_modules" (
  echo  Instalando dependencias...
  npm install
  echo.
)

start "" "http://localhost:3000"
echo  Servidor activo. Ctrl+C para detener.
echo.
node server.js
pause
