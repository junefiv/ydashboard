"""
이감(yigam.co.kr) 로그인 후 상품 문의 수집 → product.csv
- 로그인 → menu1.php 이동 → 아코디언 행 클릭 → 작성자, 상품명, 문의내용, 답변내용, 문의날짜, 답변날짜 수집
"""

import csv
import os
import re
import time
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from yigam_env import get_yigam_password, get_yigam_username

PRODUCT_CSV = "product.csv"
PRODUCT_COLUMNS = ["작성자", "상품명", "문의내용", "답변내용", "문의날짜", "답변날짜"]

# 크롤링 주기 (분 단위)
CRAWL_INTERVAL_MINUTES = 10

# 메뉴 URL
MENU1_BASE = "https://yigam.co.kr/sisain/menu1.php"
MENU1_URL = "https://yigam.co.kr/sisain/menu1.php?&sca=&save_stx=&page={}"

# 테이블: form 안의 table tbody tr (클릭 가능한 행)
TABLE_SELECTOR = "body > div > div.main > div.content > form > div > table > tbody > tr"

# 답변날짜 유효 형식: 2026\n03-24\n10:17:51 (YYYY\nMM-DD\nHH:MM:SS)
ANSWER_DATE_PATTERN = re.compile(r"^\d{4}\n\d{2}-\d{2}\n\d{2}:\d{2}:\d{2}$")


def _has_valid_answer(row_dict):
    """답변 완료된 행만 수집: 답변내용+답변날짜(날짜형태) 둘 다 있어야 함. 미답변 행은 나중에 수집."""
    ans = (row_dict.get("답변내용") or "").strip()
    dt = (row_dict.get("답변날짜") or "").strip()
    if not ans:
        return False
    if not ANSWER_DATE_PATTERN.match(dt):
        return False
    return True


def _row_key(row_dict):
    """중복 체크용 키"""
    def s(v):
        return str(v or "").strip()
    return (
        s(row_dict.get("작성자")),
        s(row_dict.get("상품명")),
        s(row_dict.get("문의날짜")),
        s(row_dict.get("문의내용")),
    )


def _load_already_collected_keys(output_dir):
    """기존 CSV에 있는 행의 키 집합"""
    path = os.path.join(output_dir, PRODUCT_CSV)
    if not os.path.exists(path):
        return set()
    keys = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not any(str(v).strip() for v in row.values()):
                continue
            mapped = {col: row.get(col, "") for col in PRODUCT_COLUMNS}
            keys.add(_row_key(mapped))
    return keys


def _ensure_csv_with_header(output_dir):
    """CSV 파일이 없으면 헤더만 있는 파일 생성"""
    path = os.path.join(output_dir, PRODUCT_CSV)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(PRODUCT_COLUMNS)


def _append_row_to_csv(output_dir, row_dict, already_collected_keys, max_retries=3):
    """수집 1건마다 CSV에 즉시 추가 (Permission denied 시 재시도)"""
    key = _row_key(row_dict)
    if key in already_collected_keys:
        return False
    path = os.path.join(output_dir, PRODUCT_CSV)
    for attempt in range(max_retries):
        try:
            with open(path, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=PRODUCT_COLUMNS)
                writer.writerow(row_dict)
                f.flush()
                os.fsync(f.fileno())
            already_collected_keys.add(key)
            return True
        except PermissionError as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                print(f"    오류 - [Errno 13] Permission denied: '{path}' (파일이 다른 프로그램에서 열려있을 수 있습니다)")
                raise
    return False


# 아코디언 확장 후 n번째 행(n은 1-based) 데이터 추출
# - tr:nth-child(1)=1행, tr:nth-child(2)=2행 ... (행마다 1개 tr)
# - tr:nth-child(n) > td.td_writename.td_hidden_480 = 작성자
# - tr:nth-child(n) > td:nth-child(2) > a = 상품명
# - #qa_div0, #qa_div1... > div > p = 문의내용
# - <!--답변정보--> ~ <!--작성자정보--> = 답변내용
# - tr:nth-child(n) > td.td_boolean.td_hidden_640 내 모든 span = 문의날짜
_EXTRACT_ACCORDION_JS = """
var n = arguments[0];  // 1-based row index (qa_div0 = first row)
var divIdx = n - 1;

function text(sel) {
    var el = document.querySelector(sel);
    return el ? el.innerText.trim() : '';
}
function allSpanTexts(tdEl) {
    if (!tdEl) return '';
    var spans = tdEl.querySelectorAll('span');
    var parts = [];
    for (var i = 0; i < spans.length; i++) { var t = (spans[i].innerText || '').trim(); if (t) parts.push(t); }
    return parts.length ? parts.join('\\n') : (tdEl.innerText || '').trim();
}

// 1) #qa_div{idx} 기준으로 먼저 시도 (아코디언 펼쳐진 블록 내부)
var container = document.getElementById('qa_div' + divIdx);

var writer = '';
var product = '';
var inquiry = '';
var answer = '';
var inquiryDate = '';
var answerDate = '';

if (container) {
    var wEl = container.querySelector('td.td_writename.td_hidden_480');
    writer = wEl ? wEl.innerText.trim() : '';
    var pEl = container.querySelector('td:nth-child(2) a');
    if (!pEl) pEl = container.querySelector('table tbody tr td:nth-child(2) a');
    product = pEl ? pEl.innerText.trim() : '';
    var iEl = container.querySelector('div p');
    if (!iEl) iEl = container.querySelector('div');
    inquiry = iEl ? iEl.innerText.trim() : '';
    var inqTd = container.querySelector('td.td_boolean.td_hidden_640');
    inquiryDate = allSpanTexts(inqTd);
    // 답변날짜는 td에서 추출 시 문의날짜와 섞임 → 답변내용 내 [답변] 패턴에서만 추출
    // 답변내용: #qa_div 내부의 <!--답변정보--> ~ <!--작성자정보-->
    var ch = container.innerHTML;
    var asi = ch.indexOf('<!--답변정보-->');
    var aei = ch.indexOf('<!--작성자정보-->');
    if (asi !== -1 && aei !== -1 && aei > asi) {
        var seg = ch.substring(asi + '<!--답변정보-->'.length, aei);
        var tmp = document.createElement('div');
        tmp.innerHTML = seg;
        answer = tmp.innerText ? tmp.innerText.trim() : tmp.textContent.trim();
        // 답변날짜: [답변] YYYY-MM-DD HH:MM:SS 패턴에서만 추출 (td는 문의날짜와 섞여 잘못됨)
        var m = answer.match(/\\[답변\\]\\s*(\\d{4})-(\\d{2})-(\\d{2})\\s+(\\d{2}:\\d{2}:\\d{2})/);
        if (m) answerDate = m[1] + '\\n' + m[2] + '-' + m[3] + '\\n' + m[4];
    }
}

// 2) tr:nth-child(n) = n번째 행 (1행=tr:1, 2행=tr:2, ...)
// body > div > div.main > div.content > form > div > table > tbody > tr:nth-child(n)
if (!writer) writer = text('body > div > div.main > div.content > form > div > table > tbody > tr:nth-child(' + n + ') td.td_writename.td_hidden_480');
if (!writer) writer = text('form div table tbody tr:nth-child(' + n + ') td.td_writename.td_hidden_480');
if (!product) {
    var pEl = document.querySelector('body > div > div.main > div.content > form > div > table > tbody > tr:nth-child(' + n + ') td:nth-child(2) a');
    if (!pEl) pEl = document.querySelector('form div table tbody tr:nth-child(' + n + ') td:nth-child(2) a');
    product = pEl ? pEl.innerText.trim() : '';
}
if (!inquiryDate) {
    var inqTd = document.querySelector('body > div > div.main > div.content > form > div > table > tbody > tr:nth-child(' + n + ') td.td_boolean.td_hidden_640');
    if (!inqTd) inqTd = document.querySelector('form div table tbody tr:nth-child(' + n + ') td.td_boolean.td_hidden_640');
    inquiryDate = allSpanTexts(inqTd);
}
// answerDate는 td에서 추출 시 잘못된 값(문의날짜)이 들어감 → [답변] 패턴에서만 사용

// 3) 답변내용: container에서 못 찾았으면 전체 페이지에서 n번째 쌍 사용
if (!answer) {
var html = document.body ? document.body.innerHTML : document.documentElement.innerHTML;
var startMark = '<!--답변정보-->';
var endMark = '<!--작성자정보-->';
var pos = 0;
for (var idx = 0; idx < n; idx++) {
    var si = html.indexOf(startMark, pos);
    var ei = html.indexOf(endMark, si);
    if (si !== -1 && ei !== -1 && ei > si) {
        if (idx === n - 1) {
            var segment = html.substring(si + startMark.length, ei);
            var tmp = document.createElement('div');
            tmp.innerHTML = segment;
            answer = tmp.innerText ? tmp.innerText.trim() : tmp.textContent.trim();
            var m = answer.match(/\\[답변\\]\\s*(\\d{4})-(\\d{2})-(\\d{2})\\s+(\\d{2}:\\d{2}:\\d{2})/);
            if (m) answerDate = m[1] + '\\n' + m[2] + '-' + m[3] + '\\n' + m[4];
        }
        pos = ei + endMark.length;
    } else break;
}
}

return { writer: writer, product: product, inquiry: inquiry, answer: answer, inquiryDate: inquiryDate, answerDate: answerDate };
"""


def _extract_accordion_data(driver, row_index_1based):
    """아코디언이 펼쳐진 상태에서 n번째 아이템 데이터 추출 (Selenium 요소 기준)"""
    try:
        raw = driver.execute_script(_EXTRACT_ACCORDION_JS, row_index_1based)
        if not raw:
            return None
        return {
            "작성자": str(raw.get("writer", "")),
            "상품명": str(raw.get("product", "")),
            "문의내용": str(raw.get("inquiry", "")),
            "답변내용": str(raw.get("answer", "")),
            "문의날짜": str(raw.get("inquiryDate", "")),
            "답변날짜": str(raw.get("answerDate", "")),
        }
    except Exception:
        return None


def _extract_accordion_data_python(driver, row_index_1based):
    """JS 실패 시 Python/Selenium으로 추출"""
    def safe_text(el):
        return (el.text or "").strip() if el else ""

    def all_span_texts(td_el):
        """td 내부 모든 span 텍스트를 개행으로 연결 (문의날짜/답변날짜 형식)"""
        if not td_el:
            return ""
        spans = td_el.find_elements(By.CSS_SELECTOR, "span")
        if spans:
            parts = [(s.text or "").strip() for s in spans if (s.text or "").strip()]
            return "\n".join(parts) if parts else safe_text(td_el)
        return safe_text(td_el)

    n = row_index_1based
    # tr:nth-child(n) = n번째 행 (1행=tr:1, 2행=tr:2, ...)

    result = {"작성자": "", "상품명": "", "문의내용": "", "답변내용": "", "문의날짜": "", "답변날짜": ""}

    try:
        # 작성자: tr:nth-child(n) td.td_writename
        for sel in [
            f"body > div > div.main > div.content > form > div > table > tbody > tr:nth-child({n}) td.td_writename.td_hidden_480",
            f"form div table tbody tr:nth-child({n}) td.td_writename.td_hidden_480",
            f"table tbody tr:nth-child({n}) td.td_writename.td_hidden_480",
        ]:
            el = driver.find_elements(By.CSS_SELECTOR, sel)
            if el:
                result["작성자"] = safe_text(el[0])
                break

        # 상품명: tr:nth-child(n) td:nth-child(2) a
        for sel in [
            f"body > div > div.main > div.content > form > div > table > tbody > tr:nth-child({n}) td:nth-child(2) a",
            f"form div table tbody tr:nth-child({n}) td:nth-child(2) a",
            f"#qa_div{n-1} td:nth-child(2) a",
        ]:
            el = driver.find_elements(By.CSS_SELECTOR, sel)
            if el:
                result["상품명"] = safe_text(el[0])
                break

        # 문의내용
        for sel in [f"#qa_div{n-1} div p", f"#qa_div{n-1} div"]:
            el = driver.find_elements(By.CSS_SELECTOR, sel)
            if el:
                result["문의내용"] = safe_text(el[0])
                break

        # 답변내용: #qa_div 내부의 <!--답변정보--> ~ <!--작성자정보--> 우선, 없으면 전체 페이지에서 n번째 쌍
        try:
            start_mark = "<!--답변정보-->"
            end_mark = "<!--작성자정보-->"
            html = ""
            qa_div = driver.find_elements(By.CSS_SELECTOR, f"#qa_div{n-1}")
            if qa_div:
                html = qa_div[0].get_attribute("innerHTML") or ""
            if html:
                si, ei = html.find(start_mark), html.find(end_mark)
                if si != -1 and ei != -1 and ei > si:
                    segment = html[si + len(start_mark) : ei]
                    result["답변내용"] = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", segment)).strip()
            if not result["답변내용"]:
                html = driver.page_source
                pos = 0
                for idx in range(n):
                    si = html.find(start_mark, pos)
                    ei = html.find(end_mark, si)
                    if si != -1 and ei != -1 and ei > si:
                        segment = html[si + len(start_mark) : ei]
                        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", segment)).strip()
                        if idx == n - 1:
                            result["답변내용"] = text
                            break
                        pos = ei + len(end_mark)
                    else:
                        break
        except Exception:
            pass

        # 문의날짜: tr:nth-child(n) td.td_boolean.td_hidden_640 내 모든 span
        for sel in [
            f"body > div > div.main > div.content > form > div > table > tbody > tr:nth-child({n}) td.td_boolean.td_hidden_640",
            f"form div table tbody tr:nth-child({n}) td.td_boolean.td_hidden_640",
            f"table tbody tr:nth-child({n}) td.td_boolean.td_hidden_640",
            f"#qa_div{n-1} td.td_boolean.td_hidden_640",
        ]:
            el = driver.find_elements(By.CSS_SELECTOR, sel)
            if el:
                result["문의날짜"] = all_span_texts(el[0])
                break

        # 답변날짜: td에서 추출 시 문의날짜와 섞임 → 답변내용의 [답변] 패턴에서만 추출
        ans_content = result.get("답변내용") or ""
        m = re.search(r"\[답변\]\s*(\d{4})-(\d{2})-(\d{2})\s+(\d{2}:\d{2}:\d{2})", ans_content)
        if m:
            result["답변날짜"] = f"{m.group(1)}\n{m.group(2)}-{m.group(3)}\n{m.group(4)}"

    except Exception:
        pass

    return result


def scrape_product_page(driver, page_num, output_dir, already_collected_keys):
    """한 페이지의 모든 아코디언 행 클릭 → 확장 후 수집"""
    driver.get(MENU1_URL.format(page_num))
    time.sleep(0.8)

    # 클릭 가능한 행: td가 있는 tr (아코디언 트리거)
    rows = driver.find_elements(By.CSS_SELECTOR, TABLE_SELECTOR)
    # 메인 행만 필터 (일부 tr은 아코디언 확장 시 나타나는 하위 tr일 수 있음)
    clickable_rows = []
    for r in rows:
        if r.find_elements(By.CSS_SELECTOR, "td") and r.is_displayed():
            clickable_rows.append(r)

    print(f"    [로그] 페이지 {page_num} 로드 완료, 행 {len(clickable_rows)}개 발견 (전체 tr: {len(rows)}개)")
    if not clickable_rows:
        print(f"    [로그] 클릭 가능한 행 없음 → 건너뜀")
        return 0

    count = 0
    n = len(clickable_rows)
    for i in range(n):
        rows = driver.find_elements(By.CSS_SELECTOR, TABLE_SELECTOR)
        clickable_rows = [r for r in rows if r.find_elements(By.CSS_SELECTOR, "td") and r.is_displayed()]

        if i >= len(clickable_rows):
            print(f"    행 {i + 1}/{n}: [로그] 행 인덱스 초과 (재조회 후 {len(clickable_rows)}개) → 건너뜀")
            break

        row = clickable_rows[i]
        row_idx_1based = i + 1

        try:
            print(f"    행 {i + 1}/{n}: [1/5] 스크롤 및 클릭 시도 중...")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", row)
            time.sleep(0.1)

            # 클릭하여 아코디언 확장
            driver.execute_script("arguments[0].click();", row)
            time.sleep(0.4)
            print(f"    행 {i + 1}/{n}: [2/5] 아코디언 클릭 완료, 데이터 추출 중...")

            # JS 추출 시도, 실패 시 Python 추출
            data = _extract_accordion_data(driver, row_idx_1based)
            if data is None:
                print(f"    행 {i + 1}/{n}: [3/5] JS 추출 → None, Python 폴백 시도")
                data = _extract_accordion_data_python(driver, row_idx_1based)
            elif not data.get("문의내용") and not data.get("답변내용"):
                print(f"    행 {i + 1}/{n}: [3/5] JS 추출됐으나 문의/답변 없음, Python 폴백 시도")
                data = _extract_accordion_data_python(driver, row_idx_1based)
            else:
                print(f"    행 {i + 1}/{n}: [3/5] JS 추출 성공")

            # 유효 데이터인지 확인
            if not any([data.get("작성자"), data.get("상품명"), data.get("문의내용"), data.get("답변내용")]):
                print(f"    행 {i + 1}/{n}: [스킵] 추출 데이터 없음")
                print(f"             → 작성자='{data.get('작성자', '')}' | 상품명='{data.get('상품명', '')}' | 문의='{(data.get('문의내용') or '')[:30]}...' | 답변='{(data.get('답변내용') or '')[:30]}...'")
                time.sleep(0.1)
                continue

            # 답변 미완료 행은 스킵 (답변 날아오면 다음 크롤링에서 수집)
            if not _has_valid_answer(data):
                print(f"    행 {i + 1}/{n}: [스킵] 답변 미완료 (답변내용 없음 또는 답변날짜 비정상) → 다음 크롤링 시 재시도")
                print(f"             → 답변날짜='{data.get('답변날짜', '')[:50]}...'")
                time.sleep(0.1)
                continue

            print(f"    행 {i + 1}/{n}: [4/5] [추출값] 작성자={data.get('작성자')} | 상품명={data.get('상품명')}")
            print(f"             문의날짜={data.get('문의날짜')} | 답변날짜={data.get('답변날짜')}")
            print(f"             문의내용={(data.get('문의내용') or '')[:60]}{'...' if len(data.get('문의내용') or '') > 60 else ''}")

            try:
                appended = _append_row_to_csv(output_dir, data, already_collected_keys)
                if appended:
                    count += 1
                    print(f"    행 {i + 1}/{n}: [5/5] CSV 추가 완료 (작성자={data.get('작성자', '')[:10]}...)")
                else:
                    print(f"    행 {i + 1}/{n}: [5/5] 이미 수집됨(중복) → CSV 미추가")
            except PermissionError:
                print(f"    행 {i + 1}/{n}: [5/5] CSV 저장 실패 - Permission denied (파일 사용 중)")
            time.sleep(0.15)

        except Exception as e:
            print(f"    행 {i + 1}/{n}: [오류] {type(e).__name__}: {e}")
            print(f"             └ traceback:\n{traceback.format_exc()}")
            time.sleep(0.15)

    return count


def run_product_crawl(driver, output_dir=None):
    """상품 문의 크롤링: 1페이지부터, 새로 수집된 건이 0건이면 종료 → 다음 주기까지 대기"""
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(__file__))

    _ensure_csv_with_header(output_dir)
    already_collected_keys = _load_already_collected_keys(output_dir)

    page_num = 1
    while True:
        print(f"  페이지 {page_num} 수집 중...")
        try:
            count = scrape_product_page(driver, page_num, output_dir, already_collected_keys)
            print(f"    → {count}건 추가")
            if count == 0:
                print(f"  0건 추가됨 → 이미 수집 완료된 구간, 크롤링 종료")
                return
        except Exception as e:
            print(f"    오류: {e}")
        page_num += 1
    print("\n수집 완료.")


def login_yigam():
    """이감 사이트에 로그인하고 driver 반환"""
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
            print(f"[{cycle}회차] 상품 문의 수집 시작 (주기: {CRAWL_INTERVAL_MINUTES}분)")
            print("="*50)
            driver = login_yigam()
            # 로그인 후 menu1.php로 이동
            driver.get("https://yigam.co.kr/sisain/menu1.php?&sca=&save_stx=&page=1")
            time.sleep(1.5)
            run_product_crawl(driver)
            driver.quit()
            print(f"\n다음 수집까지 {CRAWL_INTERVAL_MINUTES}분 대기... (Ctrl+C로 종료)")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n사용자에 의해 종료됨.")
