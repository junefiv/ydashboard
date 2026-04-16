"""
한 줄 평 수집 재개용 스크립트 (yigam_comment.py 와 동일 흐름).

- `comment_crawl_config.json` 의 `start_page` 로 수집 시작 페이지를 지정합니다.
- PC를 끄거나 중단한 뒤, 콘솔에 마지막으로 출력된 페이지 번호를 확인해
  `start_page` 를 그 번호(또는 그 다음)로 바꾼 뒤 다시 실행하면 이어서 수집할 수 있습니다.
"""

import json
import os
import time

from yigam_comment import CRAWL_INTERVAL_MINUTES, login_yigam, run_comment_crawl

COMMENT_CRAWL_CONFIG = "comment_crawl_config.json"


def _load_start_page(config_dir: str) -> int:
    path = os.path.join(config_dir, COMMENT_CRAWL_CONFIG)
    if not os.path.isfile(path):
        print(
            f"  안내: {COMMENT_CRAWL_CONFIG} 가 없어 1페이지부터 시작합니다.\n"
            f"  같은 폴더에 {{\"start_page\": 숫자}} 형식으로 만들면 해당 페이지부터 시작합니다."
        )
        return 1
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        n = int(data.get("start_page", 1))
        return n if n >= 1 else 1
    except Exception as e:
        print(f"  경고: 설정 파일 읽기 실패({e}) → 1페이지부터 시작합니다.")
        return 1


if __name__ == "__main__":
    interval_sec = CRAWL_INTERVAL_MINUTES * 60
    cycle = 0
    output_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        while True:
            cycle += 1
            start_page = _load_start_page(output_dir)
            print(f"\n{'='*50}")
            print(
                f"[{cycle}회차] 한 줄 평 수집 시작 "
                f"(시작 페이지: {start_page}, 주기: {CRAWL_INTERVAL_MINUTES}분)"
            )
            print("=" * 50)
            driver = login_yigam()
            run_comment_crawl(driver, output_dir=output_dir, start_page=start_page)
            driver.quit()
            print(f"\n다음 수집까지 {CRAWL_INTERVAL_MINUTES}분 대기... (Ctrl+C로 종료)")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n사용자에 의해 종료됨.")
