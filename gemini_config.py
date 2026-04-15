# -*- coding: utf-8 -*-
"""Gemini 호출용 설정."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def get_google_genai_api_key() -> str:
    """Google AI Studio / Gemini API 키. .env 또는 환경 변수 GOOGLE_GENAI_API_KEY / GEMINI_API_KEY."""
    return (os.environ.get("GOOGLE_GENAI_API_KEY") or os.environ.get("GEMINI_API_KEY") or "").strip()


# 기본은 최신 Flash 계열 별칭. 바꾸려면 아래 문자열 또는 GEMINI_MODEL_ID 환경변수 사용 (예: gemini-2.5-flash-lite).
DEFAULT_GENAI_MODEL_ID = os.environ.get("GEMINI_MODEL_ID", "gemini-flash-latest")
