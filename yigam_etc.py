"""
이감(yigam.co.kr) 로그인 후 기타 문의 수집 → etc.csv
- 로그인 → 기타 문의 메뉴 → 모든 행 클릭 → 모달에서 API 응답 확인
- yimo_inquiry_detail_ajax.php의 data.answer가 null이면 스킵, 값 있으면 디코딩 후 수집
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

ETC_CSV = "etc.csv"
ETC_COLUMNS = ["문의번호", "작성자", "핸드폰기종", "문의내용", "문의날짜", "답변내용"]

# 크롤링 주기 (분 단위)
CRAWL_INTERVAL_MINUTES = 10

# 메뉴 URL
MENU4_BASE = "https://yigam.co.kr/sisain/menu4.php"
SIDEBAR_MENU_SELECTOR = "body > div > div.sidebar > ul > li:nth-child(4) > a"

# 테이블
TABLE_SELECTOR = "div.inq__surface--table table tbody tr"

# 모달
MODAL_PANEL = "#inqModal .inqModal__panel"
MODAL_CLOSE = "#inqModal button.inqModal__close"

# API 전체 응답 가져오기 (id로 직접 호출)
_FETCH_DETAIL_JS = """
var id = arguments[0];
var url = '/sisain/yimo_inquiry_detail_ajax.php?id=' + encodeURIComponent(id);
var xhr = new XMLHttpRequest();
xhr.open('GET', url, false);
xhr.send(null);
if (xhr.status !== 200) return null;
try {
  var resp = JSON.parse(xhr.responseText);
  if (!resp || !resp.data) return null;
  return resp.data;
} catch (e) { return null; }
"""

# 페이지가 모달 열 때 호출하는 XHR/fetch 응답 캡처 (id를 모를 때 사용)
_INJECT_XHR_CAPTURE_JS = """
if (window.__yigamCaptureInjected) return;
window.__yigamCaptureInjected = true;
window.__yigamCapture = null;
var origOpen = XMLHttpRequest.prototype.open;
var origSend = XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.open = function(method, url) {
  this._url = url;
  return origOpen.apply(this, arguments);
};
XMLHttpRequest.prototype.send = function() {
  var xhr = this;
  xhr.addEventListener('load', function() {
    if (xhr._url && xhr._url.indexOf('yimo_inquiry_detail') !== -1) {
      try {
        var resp = JSON.parse(xhr.responseText);
        if (resp && resp.data) window.__yigamCapture = resp.data;
      } catch(e) {}
    }
  });
  return origSend.apply(this, arguments);
};
if (window.fetch) {
  var origFetch = window.fetch;
  window.fetch = function(url) {
    var urlStr = typeof url === 'string' ? url : (url && url.url ? url.url : '');
    return origFetch.apply(this, arguments).then(function(r) {
      if (urlStr.indexOf('yimo_inquiry_detail') !== -1) {
        return r.clone().json().then(function(resp) {
          if (resp && resp.data) window.__yigamCapture = resp.data;
          return r;
        }).catch(function() { return r; });
      }
      return r;
    });
  };
}
"""
_GET_CAPTURED_JS = "return window.__yigamCapture || null;"
_CLEAR_CAPTURED_JS = "if(window.__yigamCapture) window.__yigamCapture = null;"


def _row_key(row_dict):
    """중복 체크용 키"""
    def s(v):
        return str(v or "").strip()
    return (
        s(row_dict.get("문의번호")),
        s(row_dict.get("작성자")),
        s(row_dict.get("문의날짜")),
    )


def _load_already_collected_keys(output_dir):
    """기존 CSV에 있는 행의 키 집합"""
    path = os.path.join(output_dir, ETC_CSV)
    if not os.path.exists(path):
        return set()
    keys = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not any(str(v).strip() for v in row.values()):
                continue
            mapped = {col: row.get(col, "") for col in ETC_COLUMNS}
            keys.add(_row_key(mapped))
    return keys


def _ensure_csv_with_header(output_dir):
    """CSV 파일이 없으면 헤더만 있는 파일 생성"""
    path = os.path.join(output_dir, ETC_CSV)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(ETC_COLUMNS)


def _append_row_to_csv(output_dir, row_dict, already_collected_keys):
    """수집 1건마다 CSV에 즉시 추가"""
    key = _row_key(row_dict)
    if key in already_collected_keys:
        return False
    path = os.path.join(output_dir, ETC_CSV)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ETC_COLUMNS)
        writer.writerow(row_dict)
        f.flush()
        os.fsync(f.fileno())
    already_collected_keys.add(key)
    return True


def _get_inquiry_id_from_row(driver, row):
    """행에서 문의 id 추출 (API 호출용)"""
    # data-id, data-wr-id 등
    for attr in ("data-id", "data-wr-id", "data-inquiry-id", "data-inquiry_id"):
        val = row.get_attribute(attr)
        if val and re.match(r"^\d+$", str(val).strip()):
            return val.strip()
    # 링크 href에서 id 파싱
    links = row.find_elements(By.CSS_SELECTOR, "a[href*='id='], a[href*='wr_id=']")
    for link in links:
        href = link.get_attribute("href") or ""
        m = re.search(r"[?&](?:id|wr_id)=(\d+)", href)
        if m:
            return m.group(1)
    # onclick에서 숫자 추출 viewDetail(719)
    onclick = row.get_attribute("onclick") or ""
    m = re.search(r"\((\d+)\)", onclick)
    if m:
        return m.group(1)
    # 첫 번째 td가 번호일 수 있음
    first_td = row.find_elements(By.CSS_SELECTOR, "td")
    if first_td and re.match(r"^\d+$", (first_td[0].text or "").strip()):
        return (first_td[0].text or "").strip()
    return None


def _get_inquiry_id_from_modal(driver):
    """모달에서 문의 id 추출 (API 호출용 fallback)"""
    try:
        modal = driver.find_element(By.CSS_SELECTOR, "#inqModal")
        for attr in ("data-id", "data-inquiry-id"):
            val = modal.get_attribute(attr)
            if val and re.match(r"^\d+$", str(val).strip()):
                return val.strip()
        inp = driver.find_elements(By.CSS_SELECTOR, "#inqModal input[name='id'], #inqModal [data-id]")
        for el in inp:
            val = el.get_attribute("value") or el.get_attribute("data-id")
            if val and re.match(r"^\d+$", str(val).strip()):
                return val.strip()
        # #emailPill 또는 #inqModal 내 숫자 id (문의번호)
        for sel in ("#emailPill", "#inqModal [id*='emailPill']", "#inqModal [id*='id']"):
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                val = (el.text or el.get_attribute("value") or "").strip()
                if re.match(r"^\d+$", val):
                    return val
            except Exception:
                pass
        # 모달 내 모든 요소에서 data-id 검색
        js = """var els = document.querySelectorAll('#inqModal [data-id]');
for(var i=0;i<els.length;i++){var v=els[i].getAttribute('data-id');if(v&&/^\\d+$/.test(v))return v;}
var pill = document.getElementById('emailPill'); 
if(pill){var t=(pill.innerText||pill.textContent||'').trim();if(/^\\d+$/.test(t))return t;}
return null;"""
        val = driver.execute_script(js)
        if val:
            return str(val)
    except Exception:
        pass
    return None


def _extract_modal_data(driver, inquiry_id, use_captured=True):
    """모달에서 데이터 추출. answer가 null이면 None, 값 있으면 row_dict 반환.
    1) 페이지가 클릭 시 호출한 API 응답을 __yigamCapture에서 읽기
    2) 없으면 inquiry_id로 직접 API 호출
    """
    wait = WebDriverWait(driver, 5)
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#inqModal")))

    api_data = None

    # 1) 페이지가 모달 열 때 호출한 API 응답이 캡처됐는지 확인 (fetch는 비동기라 짧은 대기)
    if use_captured:
        for _ in range(5):
            api_data = driver.execute_script(_GET_CAPTURED_JS)
            if api_data is not None:
                break
            time.sleep(0.2)

    # 2) 캡처 없으면 id로 직접 API 호출
    if api_data is None and inquiry_id:
        api_data = driver.execute_script(_FETCH_DETAIL_JS, inquiry_id)
    elif api_data is None:
        inquiry_id = _get_inquiry_id_from_modal(driver)
        if inquiry_id:
            api_data = driver.execute_script(_FETCH_DETAIL_JS, inquiry_id)

    if api_data is None:
        return None

    # answer가 null이면 스킵
    if api_data.get("answer") is None or api_data.get("answer") == "":
        return None

    return {
        "문의번호": str(api_data.get("id", "")),
        "작성자": str(api_data.get("nick_name", "")),
        "핸드폰기종": str(api_data.get("device_model_nm", "")),
        "문의내용": str(api_data.get("content", "")),
        "문의날짜": str(api_data.get("created_at", "")),
        "답변내용": str(api_data.get("answer", "")),
    }


def _close_modal(driver):
    """모달 닫기"""
    try:
        btn = driver.find_element(By.CSS_SELECTOR, MODAL_CLOSE)
        btn.click()
        time.sleep(0.2)
    except Exception:
        try:
            driver.execute_script("""
                var m = document.getElementById('inqModal');
                if (m) m.style.display = 'none';
                var b = document.querySelector('.modal-backdrop');
                if (b) b.remove();
                document.body.classList.remove('modal-open');
            """)
        except Exception:
            pass


def scrape_etc_page(driver, page_num, output_dir, already_collected_keys):
    """한 페이지의 모든 행 클릭 → 모달에서 API 응답 확인 → answer가 null이면 스킵, 있으면 수집"""
    driver.get(f"{MENU4_BASE}?page={page_num}")
    time.sleep(0.6)

    # 페이지가 모달 열 때 호출하는 XHR 응답 캡처 스크립트 주입
    driver.execute_script(_INJECT_XHR_CAPTURE_JS)

    rows = driver.find_elements(By.CSS_SELECTOR, TABLE_SELECTOR)
    if not rows:
        return 0

    count = 0
    n = len(rows)
    for i in range(n):
        rows = driver.find_elements(By.CSS_SELECTOR, TABLE_SELECTOR)
        if i >= len(rows):
            break
        row = rows[i]
        if not row.find_elements(By.CSS_SELECTOR, "td"):
            continue

        inquiry_id = _get_inquiry_id_from_row(driver, row)

        try:
            driver.execute_script(_CLEAR_CAPTURED_JS)  # 이전 캡처 초기화
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", row)
            time.sleep(0.08)
            driver.execute_script("arguments[0].click();", row)
            time.sleep(0.5)  # 모달/API 응답 대기

            data = _extract_modal_data(driver, inquiry_id, use_captured=True)

            # answer가 null이면 data가 None → 스킵
            if data is None:
                print(f"    행 {i + 1}/{n}: [스킵] answer=null 또는 id 없음")
                _close_modal(driver)
                time.sleep(0.15)
                continue

            # 모달에서 추출된 값 로그
            print(f"    행 {i + 1}/{n}: [추출값] 문의번호={data.get('문의번호')} | 작성자={data.get('작성자')} | 핸드폰기종={data.get('핸드폰기종')}")
            print(f"             문의날짜={data.get('문의날짜')}")
            print(f"             문의내용={(data.get('문의내용') or '')[:80]}{'...' if len(data.get('문의내용') or '') > 80 else ''}")
            print(f"             답변내용={(data.get('답변내용') or '')[:80]}{'...' if len(data.get('답변내용') or '') > 80 else ''}")

            appended = _append_row_to_csv(output_dir, data, already_collected_keys)
            if appended:
                count += 1
                print(f"    행 {i + 1}/{n}: CSV 추가 ({data.get('문의번호', '')} {data.get('작성자', '')[:10]}...)")
            _close_modal(driver)
            time.sleep(0.15)

        except Exception as e:
            print(f"    행 {i + 1}/{n}: 오류 - {e}")
            try:
                _close_modal(driver)
            except Exception:
                pass
            time.sleep(0.15)

    return count


def run_etc_crawl(driver, output_dir=None):
    """기타 문의 크롤링: 1페이지부터, 새로 수집된 건이 0건이면 종료"""
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(__file__))

    _ensure_csv_with_header(output_dir)
    already_collected_keys = _load_already_collected_keys(output_dir)

    # 기타 문의 메뉴 진입
    wait = WebDriverWait(driver, 10)
    menu_link = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, SIDEBAR_MENU_SELECTOR))
    )
    menu_link.click()
    time.sleep(1.5)

    page_num = 1
    while True:
        print(f"  페이지 {page_num} 수집 중...")
        try:
            count = scrape_etc_page(driver, page_num, output_dir, already_collected_keys)
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
            print(f"[{cycle}회차] 기타 문의 수집 시작 (주기: {CRAWL_INTERVAL_MINUTES}분)")
            print("="*50)
            driver = login_yigam()
            run_etc_crawl(driver)
            driver.quit()
            print(f"\n다음 수집까지 {CRAWL_INTERVAL_MINUTES}분 대기... (Ctrl+C로 종료)")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n사용자에 의해 종료됨.")
