"""
이감(yigam.co.kr) 로그인 자동화 스크립트
- 로그인 후 menu2 1:1문의 수집 → 1on1.csv
"""

import csv
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from yigam_env import get_yigam_password, get_yigam_username

INQUIRY_CSV = "1on1.csv"
INQUIRY_COLUMNS = ["구분", "제목", "내용", "질문등록일", "답변", "답변등록일"]

# 크롤링 주기 (분 단위) - 여기서 수정
CRAWL_INTERVAL_MINUTES = 1


def _row_key(row_dict):
    """중복 체크용 키"""
    def s(v):
        return str(v or "").strip()
    return (
        s(row_dict.get("구분")),
        s(row_dict.get("제목")),
        s(row_dict.get("질문등록일")),
    )


def _load_already_collected_keys(output_dir):
    """기존 CSV에 있는 행의 키 집합"""
    path = os.path.join(output_dir, INQUIRY_CSV)
    if not os.path.exists(path):
        return set()
    keys = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not any(str(v).strip() for v in row.values()):
                continue
            mapped = {col: row.get(col, "") for col in INQUIRY_COLUMNS}
            keys.add(_row_key(mapped))
    return keys


def _ensure_csv_with_header(output_dir):
    """CSV 파일이 없으면 헤더만 있는 파일 생성"""
    path = os.path.join(output_dir, INQUIRY_CSV)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(INQUIRY_COLUMNS)


def _append_row_to_csv(output_dir, row_dict, already_collected_keys):
    """수집 1건마다 CSV에 즉시 추가"""
    key = _row_key(row_dict)
    if key in already_collected_keys:
        return False
    path = os.path.join(output_dir, INQUIRY_CSV)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=INQUIRY_COLUMNS)
        writer.writerow(row_dict)
        f.flush()
        os.fsync(f.fileno())
    already_collected_keys.add(key)
    return True


_EXTRACT_DETAIL_JS = """
var titleEl = document.querySelector('#bo_v_title');
var spanEl = titleEl ? titleEl.querySelector('span') : null;
var gDiv = titleEl ? titleEl.innerText.trim() : '';
var gSpan = spanEl ? spanEl.innerText.trim() : '';
var title = gDiv.replace(gSpan, '').trim();

var conEl = document.querySelector('#bo_v_con');
var content = conEl ? conEl.innerText.trim() : '';

var dateEl = document.querySelector('#bo_v_info strong.bo_date');
var qDate = dateEl ? dateEl.innerText.trim() : '';

var ansEl = document.querySelector('#ans_con');
var pList = ansEl ? ansEl.querySelectorAll('p') : [];
var ansParts = [];
for (var i = 0; i < pList.length; i++) { ansParts.push(pList[i].innerText.trim()); }
var answer = ansParts.join('\\n');

var ansDateEl = document.querySelector('#ans_datetime');
var ansDate = ansDateEl ? ansDateEl.innerText.trim() : '';

return {
  category: gSpan,
  title: title,
  content: content,
  qDate: qDate,
  answer: answer,
  ansDate: ansDate
};
"""


def _extract_detail_data(driver):
    """상세 페이지에서 데이터 추출"""
    wait = WebDriverWait(driver, 5)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#bo_v_title")))
    raw = driver.execute_script(_EXTRACT_DETAIL_JS) or {}
    return {
        "구분": str(raw.get("category", "")),
        "제목": str(raw.get("title", "")),
        "내용": str(raw.get("content", "")),
        "질문등록일": str(raw.get("qDate", "")),
        "답변": str(raw.get("answer", "")),
        "답변등록일": str(raw.get("ansDate", "")),
    }


def scrape_inquiry_page(driver, page_num, output_dir, already_collected_keys):
    """한 페이지의 모든 행 클릭 → 상세 페이지 수집 → 뒤로가기 → CSV 저장"""
    driver.get(f"https://yigam.co.kr/sisain/menu2.php?page={page_num}")
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
        stat_text = ""
        td_stat = row.find_elements(By.CSS_SELECTOR, "td.td_stat")
        if td_stat:
            stat_text = (td_stat[0].text or "").strip()

        if not row.find_elements(By.CSS_SELECTOR, "td"):
            print(f"    행 {i + 1}/{n}: td 없음, 건너뜀")
            continue

        # td.td_stat가 "답변완료"인 행만 수집
        if not td_stat or "답변완료" not in stat_text:
            print(f"    행 {i + 1}/{n}: 상태='{stat_text or '(없음)'}' → 건너뜀")
            continue

        print(f"    행 {i + 1}/{n}: 상태='{stat_text}' → 수집 시도")
        # 행 내 링크 찾기 (새 페이지로 이동하는 링크)
        link = row.find_elements(By.CSS_SELECTOR, "a[href*='wr_id='], a[href*='view'], a[href]")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", row)
        time.sleep(0.08)
        if link:
            href = link[0].get_attribute("href") or ""
            if href and href != "#" and not href.startswith("javascript:"):
                driver.get(href)
            else:
                link[0].click()
        else:
            driver.execute_script("arguments[0].click();", row)

        time.sleep(0.4)
        try:
            data = _extract_detail_data(driver)
            # 답변이 비어 있으면 수집하지 않음 (답변 등록된 글만 수집)
            if not str(data.get("답변", "")).strip():
                print(f"    행 {i + 1}/{n}: 상세에 답변 없음, 건너뜀")
                continue
            appended = _append_row_to_csv(output_dir, data, already_collected_keys)
            if appended:
                count += 1
                print(f"    행 {i + 1}/{n}: CSV 추가 ({data.get('제목', '')[:30]}...)")
            else:
                print(f"    행 {i + 1}/{n}: 이미 수집됨(중복), 건너뜀")
        except Exception as e:
            print(f"    행 {i + 1}/{n}: 추출 오류 - {e}")
        finally:
            driver.back()
            time.sleep(0.3)

    return count


def run_inquiry_crawl(driver, output_dir=None):
    """1:1문의 크롤링: 1페이지부터 순차 수집, 새로 수집된 건이 0건이면 종료"""
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(__file__))

    _ensure_csv_with_header(output_dir)
    already_collected_keys = _load_already_collected_keys(output_dir)

    driver.get("https://yigam.co.kr/sisain/menu2.php?page=1")
    time.sleep(1.5)

    page_num = 1
    while True:
        print(f"  페이지 {page_num} 수집 중...")
        try:
            count = scrape_inquiry_page(driver, page_num, output_dir, already_collected_keys)
            print(f"    → {count}건 추가")
            if count == 0:
                print(f"  0건 추가됨 → 이미 수집 완료된 구간, 크롤링 종료")
                return
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
            print(f"[{cycle}회차] 1:1문의 수집 시작 (주기: {CRAWL_INTERVAL_MINUTES}분)")
            print("="*50)
            driver = login_yigam()
            run_inquiry_crawl(driver)
            driver.quit()
            print(f"\n다음 수집까지 {CRAWL_INTERVAL_MINUTES}분 대기... (Ctrl+C로 종료)")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n사용자에 의해 종료됨.")
