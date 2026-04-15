# -*- coding: utf-8 -*-
"""
cs-dashboard.html 용 로컬·배포 공용 HTTP 서버.
- 정적 파일 제공
- POST /api/gemini-report : 집계·샘플 → Gemini(JSON) → 서버에서 고정 HTML 템플릿으로 조립
- POST /api/gemini-modal-summary : 상세 모달 문의 목록 → Gemini(Markdown 텍스트) 요약
- GitHub Pages 등 다른 오리진에서 호출 시: 환경 변수 CS_DASHBOARD_CORS_ORIGIN, 프런트 window.CS_DASHBOARD_API_BASE
"""
from __future__ import annotations

import html
import json
import os
import re
import sys
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from gemini_config import DEFAULT_GENAI_MODEL_ID, get_google_genai_api_key

# GitHub Pages 등 다른 오리진에서 fetch 시 브라우저가 CORS를 요구함. 기본 `*`, 운영 시 특정 오리진 권장.
def _cors_allow_origin() -> str:
    return (os.environ.get("CS_DASHBOARD_CORS_ORIGIN") or "*").strip() or "*"


GEMINI_URL_TMPL = (
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
)
MAX_PROMPT_CHARS = 900_000
MAX_INCOMING_BODY = 8_000_000
MAX_BULLETS_PER_SECTION = 6

# Gemini JSON 응답 스키마 (구조 고정)
REPORT_JSON_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "core_trends": {
            "type": "array",
            "items": {"type": "string"},
            "description": "핵심 트렌드 요약. 짧은 완전한 문장 3~5개.",
        },
        "customer_issue_complaints": {
            "type": "array",
            "items": {"type": "string"},
            "description": "주요 고객 이슈 및 불만 포인트. 근거 있는 경우만, 2~5개.",
        },
        "improvement_suggestions": {
            "type": "array",
            "items": {"type": "string"},
            "description": "운영 및 품질 개선 제안. 실행 가능한 수준, 2~5개.",
        },
    },
    "required": ["core_trends", "customer_issue_complaints", "improvement_suggestions"],
}


def _gemini_post(body: dict) -> tuple[dict | None, str | None]:
    api_key = get_google_genai_api_key().strip()
    if not api_key:
        return None, "API 키가 비어 있습니다. gemini_config.py 또는 GOOGLE_GENAI_API_KEY 를 설정하세요."

    model = str(DEFAULT_GENAI_MODEL_ID or "gemini-flash-latest").strip()
    url = GEMINI_URL_TMPL.format(model=model, key=api_key)
    raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=raw,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            return json.loads(resp.read().decode("utf-8")), None
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
            err_json = json.loads(err_body)
            msg = err_json.get("error", {}).get("message") or err_body[:800]
        except Exception:
            msg = str(e)
        return None, f"Gemini HTTP 오류: {msg}"
    except Exception as e:
        return None, f"Gemini 요청 실패: {e}"


def _response_text(data: dict) -> tuple[str | None, str | None]:
    if data.get("error"):
        return None, str(data["error"].get("message") or data["error"])
    cands = data.get("candidates") or []
    if not cands:
        return None, "Gemini 응답에 candidates 가 없습니다."
    parts = (cands[0].get("content") or {}).get("parts") or []
    texts = [p.get("text") for p in parts if isinstance(p, dict) and p.get("text")]
    if not texts:
        fr = cands[0].get("finishReason")
        return None, f"Gemini가 본문을 반환하지 않았습니다. finishReason={fr}"
    return "\n".join(texts), None


def _parse_json_loose(text: str) -> dict | None:
    t = (text or "").strip()
    if not t:
        return None
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", t)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except json.JSONDecodeError:
            pass
    i, j = t.find("{"), t.rfind("}")
    if i >= 0 and j > i:
        try:
            return json.loads(t[i : j + 1])
        except json.JSONDecodeError:
            pass
    return None


def _normalize_str_list(v, max_n: int) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        return [v.strip()] if v.strip() else []
    if isinstance(v, list):
        out = [str(x).strip() for x in v if str(x).strip()]
        return out[:max_n]
    return []


def _render_report_html(
    tab_title: str,
    filter_start: str,
    filter_end: str,
    trends: list[str],
    issues_complaints: list[str],
    improvements: list[str],
) -> str:
    empty_msg = "제공된 데이터만으로는 이 항목을 구체적으로 나열하기 어렵습니다."

    def block(section_class: str, title: str, items: list[str], eid: str) -> str:
        if not items:
            body = f'<p class="cs-report-empty">{html.escape(empty_msg)}</p>'
        else:
            lis = "".join(
                f'<li class="cs-report-li">{html.escape(s)}</li>' for s in items
            )
            body = f'<ul class="cs-report-ul">{lis}</ul>'
        return (
            f'<section class="cs-report-section {section_class}" '
            f'aria-labelledby="{eid}-h">'
            f'<h2 class="cs-report-h2" id="{eid}-h">{html.escape(title)}</h2>'
            f"{body}</section>"
        )

    meta = (
        f'<p class="cs-report-meta">'
        f'<span class="cs-report-meta-label">조회 기간</span> '
        f'<time datetime="{html.escape(filter_start)}">{html.escape(filter_start or "—")}</time>'
        f" ~ "
        f'<time datetime="{html.escape(filter_end)}">{html.escape(filter_end or "—")}</time>'
        f'<span class="cs-report-meta-sep">·</span>'
        f'<span class="cs-report-meta-label">채널</span> '
        f"<strong>{html.escape(tab_title)}</strong></p>"
    )

    return (
        '<article class="cs-report-root">'
        f'<header class="cs-report-header">{meta}</header>'
        f'{block("cs-report-trend", "1. 핵심 트렌드 요약", trends, "cs-sec-trend")}'
        f'{block("cs-report-neg", "2. 주요 고객 이슈 및 불만 포인트", issues_complaints, "cs-sec-neg")}'
        f'{block("cs-report-improve", "3. 운영 및 품질 개선 제안", improvements, "cs-sec-improve")}'
        "</article>"
    )


def _run_gemini_report_json(user_prompt: str) -> tuple[dict | None, str | None]:
    """JSON 스키마 모드 우선, 실패·파싱 실패 시 스키마 없이 한 번 더 시도."""
    base_gen = {
        "temperature": 0.2,
        "maxOutputTokens": 4096,
        "responseMimeType": "application/json",
    }
    json_tail = (
        "\n\n## 출력 형식 (필수)\n"
        "응답은 UTF-8 JSON **한 덩어리만** 출력하세요. 마크다운·코드펜스·설명 문장 금지.\n"
        '키 이름은 정확히: "core_trends", "customer_issue_complaints", "improvement_suggestions" (영문).\n'
        "각 값은 문자열 배열. core_trends는 3~5개, 나머지는 근거에 따라 2~5개."
    )

    last_err: str | None = None
    for attempt in (0, 1):
        use_schema = attempt == 0
        gen = {**base_gen}
        if use_schema:
            gen["responseSchema"] = REPORT_JSON_SCHEMA
        prompt = user_prompt + ("" if use_schema else json_tail)
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": gen,
        }
        data, err = _gemini_post(body)
        if err:
            last_err = err
            continue
        text, err = _response_text(data)
        if err:
            last_err = err
            continue
        obj = _parse_json_loose(text or "")
        if not isinstance(obj, dict):
            last_err = "Gemini JSON 파싱에 실패했습니다."
            continue
        trends = _normalize_str_list(obj.get("core_trends"), MAX_BULLETS_PER_SECTION)
        issues = _normalize_str_list(
            obj.get("customer_issue_complaints") or obj.get("complaint_points"),
            MAX_BULLETS_PER_SECTION,
        )
        improvements = _normalize_str_list(
            obj.get("improvement_suggestions") or obj.get("positive_points"),
            MAX_BULLETS_PER_SECTION,
        )
        if not trends and not issues and not improvements:
            last_err = "레포트 JSON이 비어 있습니다."
            continue
        return {
            "core_trends": trends,
            "customer_issue_complaints": issues,
            "improvement_suggestions": improvements,
        }, None

    return None, last_err or "Gemini 호출에 실패했습니다."


def _gemini_plain_markdown(user_prompt: str) -> tuple[str | None, str | None]:
    """JSON 스키마 없이 Markdown/텍스트 본문만 받는 단발 호출."""
    body = {
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "temperature": 0.25,
            "maxOutputTokens": 8192,
        },
    }
    data, err = _gemini_post(body)
    if err:
        return None, err
    return _response_text(data)


class CsDashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def end_headers(self):
        try:
            parsed = self.path.split("?", 1)[0]
        except Exception:
            parsed = ""
        if parsed.startswith("/api/"):
            self.send_header("Access-Control-Allow-Origin", _cors_allow_origin())
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        super().end_headers()

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args))

    def _read_json_payload(self) -> tuple[dict | None, str | None]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > MAX_INCOMING_BODY:
            return None, "잘못된 Content-Length"
        try:
            return json.loads(self.rfile.read(length).decode("utf-8")), None
        except Exception as e:
            return None, f"JSON 파싱 실패: {e}"

    def do_OPTIONS(self):
        parsed = self.path.split("?", 1)[0]
        if parsed in ("/api/gemini-modal-summary", "/api/gemini-report"):
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Allow", "POST, OPTIONS")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = self.path.split("?", 1)[0]
        if parsed == "/api/gemini-modal-summary":
            self._post_gemini_modal_summary()
            return
        if parsed != "/api/gemini-report":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        payload, rerr = self._read_json_payload()
        if rerr:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": rerr})
            return

        stats_md = str(payload.get("statsMarkdown") or "")
        tab_key = str(payload.get("tabKey") or "")
        tab_title = str(payload.get("tabTitle") or tab_key)
        fs = str(payload.get("filterStart") or "")
        fe = str(payload.get("filterEnd") or "")
        rows = payload.get("rows")
        if not isinstance(rows, list):
            rows = []
        if len(rows) > 200:
            rows = rows[:200]

        user_prompt = f"""당신은 고객센터(CS) 데이터 분석 전문가입니다.

# 채널
- 탭 코드: `{tab_key}`
- 표시명: {tab_title}
- 조회 기간(대시보드 사이드바): `{fs}` ~ `{fe}`

# 집계·표 (화면 KPI·표와 일치, 신뢰 가능)
아래 Markdown에 **없는 수치를 만들지 마세요.**

```markdown
{stats_md}
```

# 개별 문의 샘플 (JSON, 길이 제한·최근 접수 위주)
```json
{json.dumps(rows, ensure_ascii=False)}
```

---

## 분석·출력 지침
제공된 통계·샘플**만** 근거로 분석하세요. 집계와 모순되면 안 됩니다.

반드시 아래 세 축으로만 정리합니다 (JSON 키와 대응).
1. **핵심 트렌드 요약** (`core_trends`) — 접수량·유형·기간 분포 등 **표에 있는 수치**를 인용해 3~5개 bullet.
2. **주요 고객 이슈 및 불만 포인트** (`customer_issue_complaints`) — 샘플·유형에서 드러나는 이슈·불만·불편. 근거가 약하면 항목 수를 줄이세요.
3. **운영 및 품질 개선 제안** (`improvement_suggestions`) — 데이터에 근거한 실행 가능한 제안. 근거가 약하면 항목 수를 줄이세요.

각 bullet은 **한 문장으로 끝나는** 완전한 한국어 문장으로 작성하세요.
"""

        if len(user_prompt) > MAX_PROMPT_CHARS:
            user_prompt = user_prompt[:MAX_PROMPT_CHARS] + "\n\n_(프롬프트 길이 제한으로 잘림)_\n"

        report_obj, err = _run_gemini_report_json(user_prompt)
        if err:
            self._send_json(HTTPStatus.BAD_GATEWAY, {"error": err})
            return

        html_out = _render_report_html(
            tab_title,
            fs,
            fe,
            report_obj["core_trends"],
            report_obj["customer_issue_complaints"],
            report_obj["improvement_suggestions"],
        )
        self._send_json(HTTPStatus.OK, {"html": html_out})

    def _post_gemini_modal_summary(self):
        payload, rerr = self._read_json_payload()
        if rerr:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": rerr})
            return
        modal_title = str(payload.get("modalTitle") or "")
        rows = payload.get("rows")
        if not isinstance(rows, list):
            rows = []
        if len(rows) > 80:
            rows = rows[:80]
        if not rows:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "요약할 문의가 없습니다."})
            return

        user_prompt = f"""당신은 고객센터(CS) 문의 분석 전문가입니다.

# 모달 제목 (사용자가 보던 목록의 맥락)
{modal_title or "(제목 없음)"}

# 문의 목록 (JSON 배열). 각 객체의 필드만 근거로 요약하세요. **없는 사실·수치를 만들지 마세요.**

```json
{json.dumps(rows, ensure_ascii=False)}
```

## 출력 형식 (필수)
- **Markdown**으로만 작성 (코드펜스로 전체를 감싸지 말 것).
- 다음 소제목을 반드시 포함:
  - `## 전체 요약` — 3~8문장.
  - `## 주요 주제·패턴` — 불릿 3~10개.
  - `## 상담·운영 참고` — 1~5문장.
- 한국어만 사용.
"""

        if len(user_prompt) > MAX_PROMPT_CHARS:
            user_prompt = user_prompt[:MAX_PROMPT_CHARS] + "\n\n_(프롬프트 길이 제한으로 잘림)_\n"

        text, err = _gemini_plain_markdown(user_prompt)
        if err:
            self._send_json(HTTPStatus.BAD_GATEWAY, {"error": err})
            return
        if not (text or "").strip():
            self._send_json(HTTPStatus.BAD_GATEWAY, {"error": "Gemini가 요약 본문을 반환하지 않았습니다."})
            return
        self._send_json(HTTPStatus.OK, {"summary": text.strip()})

    def _send_json(self, status: int, obj: dict):
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)


def main():
    port = int(os.environ.get("PORT", "8080"))
    addr = ("", port)
    httpd = HTTPServer(addr, CsDashboardHandler)
    print(f"CS 대시보드 서버: http://localhost:{port}/cs-dashboard.html")
    print("  - 정적 파일 + POST /api/gemini-report, POST /api/gemini-modal-summary")
    co = _cors_allow_origin()
    print(f"  - CORS( /api/* ): Access-Control-Allow-Origin={co!r} (환경변수 CS_DASHBOARD_CORS_ORIGIN)")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
