"""
이감(yigam.co.kr) 로그인 후 학습 Q&A 수집 → learning.csv
- 로그인 → menu3.php 이동 → 답변완료 행 클릭 → 상세 페이지에서 1depth, 2depth, 3depth, 페이지, 문항번호, 질문자, 문의내용, 답변내용, 문의날짜, 답변날짜 수집
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

LEARNING_CSV = "learning.csv"
LEARNING_COLUMNS = [
    "1depth", "2depth", "3depth", "페이지", "문항번호", "질문자",
    "문의내용", "답변내용", "문의날짜", "답변날짜"
]

# 크롤링 주기 (분 단위)
CRAWL_INTERVAL_MINUTES = 10

# 학습 Q&A 메뉴 URL
MENU3_URL = "https://yigam.co.kr/sisain/menu3.php?page={}"
MENU3_FALLBACK_SIDEBAR = "body > div > div.sidebar > ul > li:nth-child(3) > a"

# 목록 테이블: #fqalist > div:nth-child(5) > table > tbody > tr
# - td.td_stat > span == "답변완료" 인 행만
# - td.td_subject > a 클릭 → 상세 페이지


def _row_key(row_dict):
    """중복 체크용 키"""
    def s(v):
        return str(v or "").strip()
    return (
        s(row_dict.get("1depth")),
        s(row_dict.get("2depth")),
        s(row_dict.get("3depth")),
        s(row_dict.get("페이지")),
        s(row_dict.get("문항번호")),
        s(row_dict.get("질문자")),
        s(row_dict.get("문의날짜")),
        s(row_dict.get("문의내용")),
    )


def _load_already_collected_keys(output_dir):
    """기존 CSV에 있는 행의 키 집합"""
    path = os.path.join(output_dir, LEARNING_CSV)
    if not os.path.exists(path):
        return set()
    keys = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not any(str(v).strip() for v in row.values()):
                continue
            mapped = {col: row.get(col, "") for col in LEARNING_COLUMNS}
            keys.add(_row_key(mapped))
    return keys


def _ensure_csv_with_header(output_dir):
    """CSV 파일이 없으면 헤더만 있는 파일 생성"""
    path = os.path.join(output_dir, LEARNING_CSV)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(LEARNING_COLUMNS)


def _append_row_to_csv(output_dir, row_dict, already_collected_keys, max_retries=3):
    """수집 1건마다 CSV에 즉시 추가"""
    key = _row_key(row_dict)
    if key in already_collected_keys:
        return False
    path = os.path.join(output_dir, LEARNING_CSV)
    for attempt in range(max_retries):
        try:
            with open(path, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=LEARNING_COLUMNS)
                writer.writerow(row_dict)
                f.flush()
                os.fsync(f.fileno())
            already_collected_keys.add(key)
            return True
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                print(f"    오류 - [Errno 13] Permission denied: '{path}'")
                raise
    return False


def _parse_category_for_depths(category_text):
    """
    "2026 이감 파이널 패키지... > 파이널1호 > 파이널 모의고사 제3차" 형태
    -> 1depth(첫번째 > 왼쪽), 2depth(첫번째~두번째 > 사이), 3depth(두번째 > 오른쪽)
    """
    text = (category_text or "").strip()
    parts = [p.strip() for p in text.split(">", 2)]
    d1 = parts[0] if len(parts) > 0 else ""
    d2 = parts[1] if len(parts) > 1 else ""
    d3 = parts[2] if len(parts) > 2 else ""
    return d1, d2, d3


def _parse_page_and_number(page_num_text):
    """'페이지 7 / 문항번호 20' 형태 -> (7, 20) 또는 ('7', '20')"""
    text = (page_num_text or "").strip()
    page_val = ""
    num_val = ""
    m_page = re.search(r"페이지\s*(\d+)", text, re.IGNORECASE)
    m_num = re.search(r"문항번호\s*(\d+)", text, re.IGNORECASE)
    if m_page:
        page_val = m_page.group(1)
    if m_num:
        num_val = m_num.group(1)
    return page_val, num_val


def _extract_detail_page_data(driver):
    """상세 페이지에서 모든 필드 추출"""
    def safe_text(el):
        return (el.text or "").strip() if el else ""

    result = {col: "" for col in LEARNING_COLUMNS}

    try:
        # 1depth, 2depth, 3depth: #bo_v_category > span:nth-child(1)
        cat_span = driver.find_elements(By.CSS_SELECTOR, "#bo_v_category > span:nth-child(1)")
        if cat_span:
            d1, d2, d3 = _parse_category_for_depths(safe_text(cat_span[0]))
            result["1depth"] = d1
            result["2depth"] = d2
            result["3depth"] = d3

        # 페이지, 문항번호: #bo_v_category > span:nth-child(2)
        page_span = driver.find_elements(By.CSS_SELECTOR, "#bo_v_category > span:nth-child(2)")
        if page_span:
            pg, num = _parse_page_and_number(safe_text(page_span[0]))
            result["페이지"] = pg
            result["문항번호"] = num

        # 질문자: #bo_v_info > strong:nth-child(3)
        q_el = driver.find_elements(By.CSS_SELECTOR, "#bo_v_info > strong:nth-child(3)")
        if q_el:
            result["질문자"] = safe_text(q_el[0])

        # 문의내용: #bo_v_con > div (br 태그 제외한 텍스트)
        con_el = driver.find_elements(By.CSS_SELECTOR, "#bo_v_con > div")
        if con_el:
            html = con_el[0].get_attribute("innerHTML") or ""
            text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
            text = re.sub(r"<[^>]+>", "", text)
            result["문의내용"] = re.sub(r"\s+", " ", text).strip()

        # 답변내용: #ans_con
        ans_el = driver.find_elements(By.CSS_SELECTOR, "#ans_con")
        if ans_el:
            result["답변내용"] = safe_text(ans_el[0])

        # 문의날짜: #bo_v_info > strong.bo_date
        inq_date_el = driver.find_elements(By.CSS_SELECTOR, "#bo_v_info > strong.bo_date")
        if inq_date_el:
            result["문의날짜"] = safe_text(inq_date_el[0])

        # 답변날짜: #ans_datetime
        ans_date_el = driver.find_elements(By.CSS_SELECTOR, "#ans_datetime")
        if ans_date_el:
            result["답변날짜"] = safe_text(ans_date_el[0])

    except Exception as e:
        print(f"        [추출 오류] {e}")
    return result


def _navigate_to_menu3(driver):
    """
    로그인 후 menu3.php?page=1 로 이동.
    URL이 유효하지 않으면(예: 로그인 페이지로 리다이렉트) sidebar li:nth-child(3) 클릭.
    """
    target_url = MENU3_URL.format(1)
    driver.get(target_url)
    time.sleep(1.5)

    current_url = driver.current_url
    # 로그인 페이지 또는 menu3가 아닌 경우 메뉴 클릭 시도
    if "menu3" not in current_url or "login" in current_url.lower() or "sisain" not in current_url:
        try:
            link = driver.find_element(By.CSS_SELECTOR, MENU3_FALLBACK_SIDEBAR)
            link.click()
            time.sleep(1.5)
        except Exception as e:
            print(f"    [경고] sidebar 메뉴 클릭 실패: {e}")


def scrape_learning_page(driver, page_num, output_dir, already_collected_keys):
    """
    한 페이지에서 답변완료 행만 필터 → 각 행의 링크 클릭 → 상세 페이지 수집 → 뒤로가기 → 다음 행
    """
    driver.get(MENU3_URL.format(page_num))
    time.sleep(1)

    # #fqalist > div:nth-child(5) > table > tbody > tr 목록
    table_sel = "#fqalist > div:nth-child(5) > table > tbody > tr"
    rows = driver.find_elements(By.CSS_SELECTOR, table_sel)

    # 대안 셀렉터 (구조가 다를 수 있음)
    if not rows:
        rows = driver.find_elements(By.CSS_SELECTOR, "#fqalist table tbody tr")
    if not rows:
        rows = driver.find_elements(By.CSS_SELECTOR, ".fqalist table tbody tr")

    print(f"    [로그] 페이지 {page_num} 로드, 행 {len(rows)}개 발견")
    if not rows:
        return 0

    count = 0
    n = len(rows)

    for i in range(n):
        rows = driver.find_elements(By.CSS_SELECTOR, table_sel)
        if not rows:
            rows = driver.find_elements(By.CSS_SELECTOR, "#fqalist table tbody tr")

        if i >= len(rows):
            break

        row = rows[i]
        row_idx = i + 1

        try:
            # td.td_stat > span == "답변완료" 인지 확인
            stat_td = row.find_elements(By.CSS_SELECTOR, "td.td_stat span")
            if not stat_td:
                stat_td = row.find_elements(By.CSS_SELECTOR, "td.td_stat")
            status_text = ""
            if stat_td:
                status_text = (stat_td[0].text or "").strip()

            if "답변완료" not in status_text:
                continue

            # td.td_subject > a 클릭
            link_el = row.find_elements(By.CSS_SELECTOR, "td.td_subject a")
            if not link_el:
                link_el = row.find_elements(By.CSS_SELECTOR, "td.td_subject a")
            if not link_el:
                link_el = row.find_elements(By.CSS_SELECTOR, "a")
            if not link_el:
                print(f"    행 {row_idx}/{n}: [스킵] 링크 없음")
                continue

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link_el[0])
            time.sleep(0.15)

            link_el[0].click()
            time.sleep(0.8)

            data = _extract_detail_page_data(driver)
            data["페이지"] = data.get("페이지") or str(page_num)

            if not any([data.get("질문자"), data.get("문의내용"), data.get("답변내용")]):
                print(f"    행 {row_idx}/{n}: [스킵] 상세 추출 데이터 없음")
                driver.back()
                time.sleep(0.5)
                continue

            appended = _append_row_to_csv(output_dir, data, already_collected_keys)
            if appended:
                count += 1
                print(f"    행 {row_idx}/{n}: [수집] 1depth={data.get('1depth','')[:20]}... | 질문자={data.get('질문자','')}")

            driver.back()
            time.sleep(0.6)

        except Exception as e:
            print(f"    행 {row_idx}/{n}: [오류] {type(e).__name__}: {e}")
            traceback.print_exc()
            try:
                driver.back()
                time.sleep(0.5)
            except Exception:
                pass

    return count


def run_learning_crawl(driver, output_dir=None):
    """
    학습 Q&A 크롤링: 1페이지부터, 새로 수집된 건이 0건이면 종료 → 다음 주기까지 대기
    """
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(__file__))

    _ensure_csv_with_header(output_dir)
    already_collected_keys = _load_already_collected_keys(output_dir)

    page_num = 1
    while True:
        print(f"  페이지 {page_num} 수집 중...")
        try:
            count = scrape_learning_page(driver, page_num, output_dir, already_collected_keys)
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
            print(f"[{cycle}회차] 학습 Q&A 수집 시작 (주기: {CRAWL_INTERVAL_MINUTES}분)")
            print("="*50)
            driver = login_yigam()
            _navigate_to_menu3(driver)
            run_learning_crawl(driver)
            driver.quit()
            print(f"\n다음 수집까지 {CRAWL_INTERVAL_MINUTES}분 대기... (Ctrl+C로 종료)")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n사용자에 의해 종료됨.")
