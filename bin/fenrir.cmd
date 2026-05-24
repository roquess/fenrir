@echo off
setlocal
set ROOT=%~dp0..
erl -pa "%ROOT%\_build\default\lib\*\ebin" -noshell -run fenrir_cli main %*
