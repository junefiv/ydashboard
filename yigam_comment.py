"""
이감(yigam.co.kr) 로그인 자동화 스크립트
- 로그인 후 한 줄 평 수집 → comment.csv
"""

import csv
import os
import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from yigam_env import get_yigam_password, get_yigam_username

COMMENT_CSV = "comment.csv"
COMMENT_COLUMNS = ["입시년도", "모의고사", "선택과목", "구분", "시즌", "회차", "작성자", "본문", "등록일"]

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
    """한 페이지의 모든 데이터 행 클릭 → 모달 열고 수집 → CSV 저장"""
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
