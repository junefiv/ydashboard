"""
이감(yigam.co.kr) 로그인 자동화 스크립트
- 로그인 후 한 줄 평 수집 → comment.csv
- 수집 시 본문 기준 Gemini로 감정 카테고리(6분류) 라벨링
"""

import csv
import json
import os
import re
import time
import urllib.error
import urllib.request
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from gemini_config import DEFAULT_GENAI_MODEL_ID, get_google_genai_api_key
from yigam_env import get_yigam_password, get_yigam_username

COMMENT_CSV = "comment.csv"
COMMENT_COLUMNS = [
    "입시년도",
    "모의고사",
    "선택과목",
    "구분",
    "시즌",
    "회차",
    "작성자",
    "본문",
    "감정카테고리",
    "등록일",
]

GEMINI_URL_TMPL = (
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
)

# 사용자 정의 감정 분류(정의·분석 포인트는 모델 프롬프트에만 사용, CSV에는 카테고리명만 저장)
_EMOTION_TAXONOMY = [
    {
        "emotion_category": "당혹/고난",
        "definition": "예상보다 높은 난이도나 생소한 문제 유형으로 인해 사용자가 당황하거나 심리적 압박감을 느끼는 상태입니다. 주로 시간 부족, 특정 지문의 난해함, 혹은 '멘탈 붕괴'와 같은 표현으로 나타납니다.",
        "analysis_point": "시험 콘텐츠의 난이도 조절 적정성을 평가하는 핵심 지표입니다. 사용자가 어느 지점에서 병목 현상을 느끼는지 식별하여, 향후 학습 가이드 제공이나 난이도 밸런싱을 위한 데이터로 활용합니다.",
    },
    {
        "emotion_category": "좌절/자책",
        "definition": "학습 성과가 기대에 미치지 못하거나 실수를 저질렀을 때 느끼는 슬픔과 스스로에 대한 비난이 섞인 상태입니다. 무력감이나 부모님에 대한 미안함 등 깊은 감정적 소모를 특징으로 합니다.",
        "analysis_point": "유저의 서비스 이탈 가능성이 가장 높은 지점을 파악합니다. 수험생의 회복 탄력성을 높이기 위한 심리적 케어 콘텐츠의 필요성을 진단하고, 리텐션을 강화하기 위한 동기부여 전략 수립의 근거로 사용합니다.",
    },
    {
        "emotion_category": "성취/만족",
        "definition": "목표 점수 달성, 실력 향상 체감, 또는 문제의 질에 대해 긍정적인 효능감을 느끼는 상태입니다. '성공적인 학습 경험'이 발현된 상태로, 서비스에 대한 신뢰와 즐거움이 직접적으로 드러납니다.",
        "analysis_point": "긍정적 사용자 경험(UX)의 성공 요인을 분석합니다. 어떤 유형의 문항이나 서비스 요소가 유저에게 높은 만족도를 주는지 파악하여, 이를 브랜드 마케팅 포인트로 활용하고 로열티를 강화하는 선순환 구조를 구축합니다.",
    },
    {
        "emotion_category": "분노/불만",
        "definition": "문제의 퀄리티, 출제 오류 의혹, 혹은 지나치게 작위적인 출제 방식(사설틱)에 대해 강한 거부감과 불쾌감을 표출하는 상태입니다. 시험의 공정성이나 실효성에 대해 공격적인 성향을 띠기도 합니다.",
        "analysis_point": "콘텐츠 제작 및 검수 프로세스의 결함을 식별하는 결정적인 지표입니다. 부정적 구전(Viral)을 유발하는 요소를 신속히 파악하여 콘텐츠 고도화에 반영하고, 브랜드 이미지 훼손을 최소화하기 위한 선제적 대응 근거로 활용합니다.",
    },
    {
        "emotion_category": "의지/희망",
        "definition": "결과에 매몰되지 않고 다음 목표를 향한 다짐을 하거나, 동료 수험생들과 서로 격려하며 긍정적인 에너지를 공유하는 상태입니다. 미래 지향적인 태도와 공동체 의식이 강조됩니다.",
        "analysis_point": "커뮤니티 내의 긍정적인 상호작용과 유저의 학습 의지를 측정합니다. 이러한 능동적인 유저층이 서비스 생태계에 미치는 선한 영향력을 분석하여, 커뮤니티 활성화 및 장기적인 서비스 이용 패턴을 예측하는 데 활용합니다.",
    },
    {
        "emotion_category": "중립/평가",
        "definition": "감정적인 동요 없이 지문의 특징, 오답률, 등급컷 예측 등 시험의 객관적인 정보를 분석하거나 사실 관계를 기록하는 상태입니다. 정보 전달 중심의 담백한 소통이 특징입니다.",
        "analysis_point": "유저들이 가장 객관적으로 서비스를 바라보는 지점을 확인합니다. 감정적 노이즈를 배제한 순수 피드백 데이터를 추출하여 문제 개선의 우선순위를 정하거나, 전체적인 시장 반응을 정량적으로 분석하는 기저 지표로 활용합니다.",
    },
]

_EMOTION_LABELS = [x["emotion_category"] for x in _EMOTION_TAXONOMY]

_EMOTION_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "emotion_category": {
            "type": "string",
            "enum": _EMOTION_LABELS,
            "description": "본문에 가장 잘 맞는 단일 감정 카테고리",
        },
    },
    "required": ["emotion_category"],
}


def _parse_json_loose(text: str):
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


def _gemini_post(body: dict):
    api_key = get_google_genai_api_key().strip()
    if not api_key:
        return None, "API 키가 비어 있습니다. .env 의 GOOGLE_GENAI_API_KEY 를 설정하세요."
    model = str(DEFAULT_GENAI_MODEL_ID or "gemma-4-26b-a4b-it").strip()
    url = GEMINI_URL_TMPL.format(model=model, key=api_key)
    raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=raw,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
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


def _response_text(data: dict):
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


def classify_comment_emotion(body: str, verbose: bool = True) -> str:
    """본문 텍스트를 6개 emotion_category 중 하나로 분류. 실패·빈 문자열 시 빈 문자열."""
    text = (body or "").strip()
    if not text:
        return ""
    if len(text) > 6000:
        text = text[:6000]
    taxonomy_json = json.dumps(_EMOTION_TAXONOMY, ensure_ascii=False, indent=2)
    prompt = (
        "당신은 수험생 커뮤니티 '한 줄 평' 댓글 분류기입니다.\n"
        "아래 JSON 배열은 감정 카테고리별 definition과 analysis_point입니다. "
        "definition과 analysis_point를 참고해 본문의 톤·내용에 가장 맞는 카테고리를 **정확히 하나**만 고르세요.\n\n"
        f"## 분류 기준 (JSON)\n{taxonomy_json}\n\n"
        "## 분류할 본문\n"
        f"{text}\n\n"
        "출력은 스키마대로 JSON만 (emotion_category 필드 하나). "
        "본문이 정보 나열 위주이고 뚜렷한 감정이 없으면 '중립/평가'를 선택하세요."
    )
    body_req = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.15,
            "maxOutputTokens": 1024,
            "responseMimeType": "application/json",
            "responseSchema": _EMOTION_JSON_SCHEMA,
        },
    }
    data, err = _gemini_post(body_req)
    if err:
        if verbose:
            print(f"    [감정분류] {err}")
        return ""
    raw_txt, err = _response_text(data)
    if err:
        if verbose:
            print(f"    [감정분류] {err}")
        return ""
    obj = _parse_json_loose(raw_txt or "")
    if not isinstance(obj, dict):
        return ""
    cat = str(obj.get("emotion_category", "")).strip()
    if cat in _EMOTION_LABELS:
        return cat
    return ""


def _migrate_comment_csv_schema(output_dir):
    """기존 comment.csv 헤더가 COMMENT_COLUMNS와 다르면 열 맞춤 후 전체 재작성."""
    path = os.path.join(output_dir, COMMENT_CSV)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if tuple(fieldnames) == tuple(COMMENT_COLUMNS):
            return
        rows_in = list(reader)

    def _normalize_reader_row(r: dict) -> dict:
        mapped = {col: (r.get(col) if r.get(col) is not None else "") for col in COMMENT_COLUMNS}
        if not str(mapped.get("본문", "")).strip() and r.get("한 줄 평"):
            mapped["본문"] = str(r.get("한 줄 평", ""))
        if not str(mapped.get("등록일", "")).strip() and r.get("작성일"):
            mapped["등록일"] = str(r.get("작성일", ""))
        return mapped

    rows_out = [_normalize_reader_row(r) for r in rows_in]
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COMMENT_COLUMNS)
        w.writeheader()
        w.writerows(rows_out)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    print(f"  기존 {COMMENT_CSV} 헤더를 최신 열 구성으로 맞췄습니다 (감정카테고리 열 추가 등).")


# 크롤링 주기 (분 단위) - 여기서 수정
CRAWL_INTERVAL_MINUTES = 10


def _row_key(row_dict):
    """중복 체크용 키 (이미 수집된 행 판단, strip 적용)"""
    def s(v):
        return str(v or "").strip()
    return (
        s(row_dict.get("입시년도")),
        s(row_dict.get("모의고사")),
        s(row_dict.get("선택과목")),
        s(row_dict.get("작성자")),
        s(row_dict.get("등록일")),
        s(row_dict.get("본문")),
    )


def _load_already_collected_keys(output_dir):
    """기존 CSV에 있는 행의 키 집합 (재실행 시 수집 중단 기준)"""
    path = os.path.join(output_dir, COMMENT_CSV)
    if not os.path.exists(path):
        return set()
    keys = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not any(str(v).strip() for v in row.values()):
                continue
            mapped = {col: row.get(col, "") for col in COMMENT_COLUMNS}
            if "한 줄 평" in row and not mapped.get("본문"):
                mapped["본문"] = row.get("한 줄 평", "")
            if "작성일" in row and not mapped.get("등록일"):
                mapped["등록일"] = row.get("작성일", "")
            keys.add(_row_key(mapped))
    return keys


def _ensure_csv_with_header(output_dir):
    """CSV 파일이 없으면 헤더만 있는 파일 생성"""
    path = os.path.join(output_dir, COMMENT_CSV)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(COMMENT_COLUMNS)


def _append_row_to_csv(output_dir, row_dict, already_collected_keys):
    """수집 1건마다 CSV에 즉시 추가 (이미 수집된 행은 절대 추가하지 않음)"""
    key = _row_key(row_dict)
    if key in already_collected_keys:
        return False
    path = os.path.join(output_dir, COMMENT_CSV)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COMMENT_COLUMNS)
        writer.writerow(row_dict)
        f.flush()
        os.fsync(f.fileno())
    already_collected_keys.add(key)
    return True


def _read_comment_csv_rows(output_dir):
    """comment.csv 전체 데이터 행을 COMMENT_COLUMNS 형태로 읽기 (헤더만·빈 행 제외)."""
    path = os.path.join(output_dir, COMMENT_CSV)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    rows = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row or not any(str(v).strip() for v in row.values()):
                continue
            mapped = {col: (row.get(col) if row.get(col) is not None else "") for col in COMMENT_COLUMNS}
            if "한 줄 평" in row and not str(mapped.get("본문", "")).strip():
                mapped["본문"] = str(row.get("한 줄 평", ""))
            if "작성일" in row and not str(mapped.get("등록일", "")).strip():
                mapped["등록일"] = str(row.get("작성일", ""))
            rows.append(mapped)
    return rows


def _write_comment_csv_rows(output_dir, rows):
    """comment.csv 전체 덮어쓰기 (원자적 교체)."""
    path = os.path.join(output_dir, COMMENT_CSV)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COMMENT_COLUMNS)
        w.writeheader()
        w.writerows(rows)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _probe_and_backfill_unlabeled_emotions(output_dir) -> int:
    """
    페이지마다: 본문은 있는데 감정카테고리가 비어 있는 행이 있으면,
    그중 첫 본문으로 API 가동 여부를 프로브하고, 성공 시 같은 파일 내 미라벨을 순서대로 채운 뒤 저장.
    프로브 실패(할당량 등)면 저장 없이 0 반환 — 크롤은 그대로 진행.
    반환: 이번에 새로 채워 넣은 행 수.
    """
    rows = _read_comment_csv_rows(output_dir)
    idxs = [
        i
        for i, r in enumerate(rows)
        if str(r.get("본문", "")).strip() and not str(r.get("감정카테고리", "")).strip()
    ]
    if not idxs:
        return 0
    first_body = str(rows[idxs[0]]["본문"]).strip()
    probe = classify_comment_emotion(first_body, verbose=True)
    if not probe:
        print("    [감정보강] 미라벨 프로브 실패(할당량·오류 등) → 백필 생략, 크롤만 진행.")
        return 0
    rows[idxs[0]]["감정카테고리"] = probe
    filled = 1
    for j in idxs[1:]:
        b = str(rows[j].get("본문", "")).strip()
        if not b:
            continue
        lab = classify_comment_emotion(b, verbose=False)
        if not lab:
            print(
                f"    [감정보강] 중간 API 실패 → 지금까지 {filled}건만 반영하고 저장합니다."
            )
            break
        rows[j]["감정카테고리"] = lab
        filled += 1
    _write_comment_csv_rows(output_dir, rows)
    print(f"    [감정보강] 미라벨 {filled}건 감정카테고리 보강 저장.")
    return filled


# JS로 9개 필드 한 번에 추출 (round-trip 1회)
_EXTRACT_JS = """
var g=function(s){var e=document.querySelector(s);return e?e.innerText.trim():'';};
return {
  mYear:g('#mYear'),mExam:g('#mExam'),mSubject:g('#mSubject'),
  mType:g('#mType'),mSeason:g('#mSeason'),mRound:g('#mRound'),
  mWriter:g('#mWriter'),mContent:g('#mContent'),mDate:g('#mDate')
};
"""


def _extract_modal_data(driver):
    """모달 열리면 JS로 한 번에 값 추출 (최소 대기)"""
    wait = WebDriverWait(driver, 2)
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#reviewModal #mYear")))
    raw = driver.execute_script(_EXTRACT_JS) or {}
    m_writer = re.sub(r"^작성자\s*", "", str(raw.get("mWriter", "")))
    m_date = re.sub(r"^등록일\s*", "", str(raw.get("mDate", "")))
    return {
        "입시년도": str(raw.get("mYear", "")),
        "모의고사": str(raw.get("mExam", "")),
        "선택과목": str(raw.get("mSubject", "")),
        "구분": str(raw.get("mType", "")),
        "시즌": str(raw.get("mSeason", "")),
        "회차": str(raw.get("mRound", "")),
        "작성자": m_writer,
        "본문": str(raw.get("mContent", "")),
        "감정카테고리": "",
        "등록일": m_date,
    }


def _close_modal(driver):
    """모달 닫기 버튼 클릭"""
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "#reviewModal .inqModal__close")
        btn.click()
        time.sleep(0.2)
    except Exception:
        try:
            driver.execute_script("""var m=document.getElementById('reviewModal');if(m)m.style.display='none';
                var b=document.querySelector('.modal-backdrop');if(b)b.remove();document.body.classList.remove('modal-open');""")
        except Exception:
            pass


def scrape_comment_page(driver, page_num, output_dir, already_collected_keys):
    """한 페이지: 미라벨 보강(가능 시) → 해당 페이지 모달 수집 → CSV 저장"""
    _probe_and_backfill_unlabeled_emotions(output_dir)

    driver.get(f"https://yigam.co.kr/sisain/menu5.php?page={page_num}")
    time.sleep(0.6)

    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    if not rows:
        return 0

    n = len(rows)
    count = 0
    for i in range(n):
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if i >= len(rows):
            break
        row = rows[i]
        if not row.find_elements(By.CSS_SELECTOR, "td"):
            continue
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", row)
            time.sleep(0.08)
            driver.execute_script("arguments[0].click();", row)
            time.sleep(0.25)
            data = _extract_modal_data(driver)
            body = str(data.get("본문", "")).strip()
            data["감정카테고리"] = classify_comment_emotion(body) if body else ""
            appended = _append_row_to_csv(output_dir, data, already_collected_keys)
            if appended:
                count += 1
            _close_modal(driver)
            time.sleep(0.15)
        except Exception as e:
            try:
                _close_modal(driver)
            except Exception:
                pass
    return count


def run_comment_crawl(driver, output_dir=None):
    """한 줄 평 크롤링: 1페이지부터 순차 수집, 이미 수집된 행 만나면 중단"""
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(__file__))

    _ensure_csv_with_header(output_dir)
    _migrate_comment_csv_schema(output_dir)
    if not get_google_genai_api_key().strip():
        print("  경고: GOOGLE_GENAI_API_KEY 가 없습니다. 감정카테고리는 빈 칸으로 저장됩니다.")
    already_collected_keys = _load_already_collected_keys(output_dir)

    wait = WebDriverWait(driver, 10)
    menu_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-flex[href*='menu5.php']")))
    menu_link.click()
    time.sleep(1.5)

    page_num = 1
    while True:
        print(f"  페이지 {page_num} 수집 중...")
        try:
            count = scrape_comment_page(driver, page_num, output_dir, already_collected_keys)
            print(f"    → {count}건 추가 (페이지 내 모든 행 모달 열어서 수집)")
            if count == 0:
                print(f"  0건 추가됨 → 이미 수집 완료된 구간, 크롤링 종료")
                break
        except Exception as e:
            print(f"    오류: {e}")
        page_num += 1
    print("\n수집 완료.")


def login_yigam():
    """이감 사이트에 로그인합니다."""
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get("https://yigam.co.kr/sisain")

        wait = WebDriverWait(driver, 10)

        username = get_yigam_username()
        password = get_yigam_password()
        if not username or not password:
            raise ValueError("YIGAM_USERNAME / YIGAM_PASSWORD 를 .env에 설정하세요.")

        username_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#username"))
        )
        username_input.clear()
        username_input.send_keys(username)

        password_input = driver.find_element(By.CSS_SELECTOR, "#password")
        password_input.clear()
        password_input.send_keys(password)

        login_button = driver.find_element(
            By.CSS_SELECTOR, 'button.btn-login[onclick="submit()"]'
        )
        login_button.click()

        time.sleep(2)
        print("로그인 완료!")
        return driver

    except Exception as e:
        print(f"오류 발생: {e}")
        raise


if __name__ == "__main__":
    interval_sec = CRAWL_INTERVAL_MINUTES * 60
    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\n{'='*50}")
            print(f"[{cycle}회차] 한 줄 평 수집 시작 (주기: {CRAWL_INTERVAL_MINUTES}분)")
            print("="*50)
            driver = login_yigam()
            run_comment_crawl(driver)
            driver.quit()
            print(f"\n다음 수집까지 {CRAWL_INTERVAL_MINUTES}분 대기... (Ctrl+C로 종료)")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n사용자에 의해 종료됨.")
