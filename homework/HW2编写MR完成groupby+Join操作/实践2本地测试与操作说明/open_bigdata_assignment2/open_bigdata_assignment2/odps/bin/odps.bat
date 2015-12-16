@echo off
mode con cols=125 lines=2000
title ODPS
SETLOCAL ENABLEDELAYEDEXPANSION
set "BASE_DIR=%~dp0"
pushd "%BASE_DIR%"

set MAIN_CLASS=com.aliyun.odps.cli.Main
set "LIB_DIR=%BASE_DIR%..\lib\"

set "CLASSPATH=."
for %%F in ("%LIB_DIR%*.jar") do (
set "CLASSPATH=!CLASSPATH!;%%F"
)

set "CONF_DIR=%cd%\..\conf\"
set "CLASSPATH=!CLASSPATH!;%CONF_DIR%"

rem set java env
popd
java -Xms64m -Xmx1024m  %MAIN_CLASS% %*

ENDLOCAL