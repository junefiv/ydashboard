@echo off
chcp 65001 >nul
title CS 대시보드 - 로컬 서버
cd /d "%~dp0"

echo.
echo ========================================
echo   고객센터 1:1 문의 종합 대시보드
echo ========================================
echo.
echo [1] 로컬 서버를 시작합니다. (포트 8080, Gemini API 포함)
echo [2] 같은 폴더의 CSV 5종을 대시보드가 읽습니다.
echo.
echo 대시보드 주소: http://localhost:8080/cs-dashboard.html
echo.
echo 종료하려면 이 창을 닫거나 Ctrl+C 를 누르세요.
echo ========================================
echo.

:: 2초 후 브라우저 자동 열기
start /b cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8080/cs-dashboard.html"

python serve_cs_dashboard.py
pause
