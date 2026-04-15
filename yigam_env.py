# -*- coding: utf-8 -*-
"""프로젝트 루트의 .env 로드 및 이감 로그인용 환경 변수."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def get_yigam_username() -> str:
    return (os.environ.get("YIGAM_USERNAME") or "").strip()


def get_yigam_password() -> str:
    return (os.environ.get("YIGAM_PASSWORD") or "").strip()
