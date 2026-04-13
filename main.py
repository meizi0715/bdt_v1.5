import os
import re
import json
import asyncio
import smtplib
import hashlib
import calendar
import jpholiday
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from collections import defaultdict
from email.mime.text import MIMEText
from playwright.async_api import Frame
from playwright.async_api import async_playwright
#===========v1.6 2026/03/10 Add Start
from googleapiclient.discovery import build
from google.oauth2 import service_account
#===========v1.6 2026/03/10 Add End


OUTPUT_DIR = "output"
# OUTPUT_DIR = "C:/Users/xxx/Downloads/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

time_slots = {
    '0': '09:00～11:00',
    '1': '11:00～13:00',
    '2': '13:00～15:00',
    '3': '15:00～17:00',
    '4': '17:00～19:00',
}

SCC = json.loads(os.getenv("SCC_JSON"))
email_config = json.loads(os.getenv("EMAIL_CONFIG"))
web_ele = json.loads(os.getenv("WEB_ELE"))

#===========v1.6 2026/03/10 Add Start
service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
calendars = json.loads(os.getenv("CALENDARS_JSON"))
count_calendar_id = os.getenv("COUNT_CALENDAR_ID")
#===========v1.6 2026/03/10 Add End

def get_end_of_next_month(today: date = None) -> date:
    if today is None:
        today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    year = today.year
    month = today.month + 1
    if month > 12:
        year += 1
        month = 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)

#===========v1.4 2025/12/29 Add Start
def get_end_of_month_after_next(today: date = None) -> date:
    # 翌々月末
    if today is None:
        today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    year = today.year
    month = today.month + 2
    if month > 12:
        year += (month - 1) // 12
        month = month % 12 if month % 12 != 0 else 12
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)

def get_date_n_weeks_later(today: date, weeks: int) -> date:
    # N週間後の日付
    if today is None:
        today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    return today + timedelta(weeks=weeks)
#===========v1.4 2025/12/29 Add End

def extract_date(text: str, year: int = None) -> date:
    match = re.search(r"(\d{1,2})月(\d{1,2})日", text)
    if not match:
        raise ValueError("无法从字符串中提取日期")
    month = int(match.group(1))
    day = int(match.group(2))
    
    if year is None:    
        today = datetime.now(ZoneInfo("Asia/Tokyo"))
        year = today.year
        if today.month in [11, 12] and month in [1, 2]:
            year += 1
            
    return date(year, month, day)

def weekend_or_holiday(date: datetime.date) -> bool:
    return date.weekday() >= 5 or jpholiday.is_holiday(date)

def get_timestamp():
    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    # now = datetime.now()
    rounded = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    return rounded.strftime("%Y%m%d%H%M")

def save_file(lines: list[str], filename: str):
    with open(filename, "w", encoding="utf-8") as f:
        for line in lines:
            if isinstance(line, str):
                f.write(line + "\n")
            elif isinstance(line, list):
                # 展開リスト中の每个元素（即使只有一个）
                for subline in line:
                    if isinstance(subline, str):
                        f.write(subline + "\n")
                    else:
                        print(f"⚠️ 跳过非字符串子元素: {subline}")
            else:
                print(f"⚠️ 跳过非字符串行: {line}")

def compare_files(file1: str, file2: str) -> bool:
    def hash_file(path):
        with open(path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    return hash_file(file1) != hash_file(file2)

def send_mail(body_lines: list[str], has_avali: bool = True):

    today_schedule = []
    today_schedule = get_today_schedule()

    if has_avali and body_lines:
        all_lines = today_schedule + [email_config["line3"]] + body_lines        
    else:
        all_lines = today_schedule + [email_config["line3"]] + [email_config["noavali"]] + [email_config["line0"]]
        
    email_body = email_config["header"] + all_lines[0] + "\n" + "\n".join(all_lines[1:] + [email_config["footer"]])
        
    msg = MIMEText(email_body, "plain", "utf-8")

    today = datetime.now(ZoneInfo("Asia/Tokyo"))
    msg["Subject"] = f"{email_config['subject']}({today.strftime('%m/%d')})"
    msg["From"] = email_config["from"]
    msg["To"] = email_config["to"]

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(email_config["from"], email_config["pass"])
        server.send_message(msg)

    #===========v1.7 2026/03/17 Add Start
    # 送信日付を記録
    today_str = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y%m%d")
    sent_flag_file = os.path.join(OUTPUT_DIR, "daily_sent.txt")
    with open(sent_flag_file, "w") as f:
        f.write(today_str)
    #===========v1.7 2026/03/17 Add End

async def wait_for_html_change(frame: Frame, selector: str, old_html: str, centername: str, timeout: int = 25000, interval: int = 500) -> str:
    elapsed = 0
    while elapsed < timeout:
        new_html = await frame.locator(selector).inner_html()
        if new_html != old_html:
            return new_html
        await frame.wait_for_timeout(interval)
        elapsed += interval

    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    # now = datetime.now()
    print(f"{now.strftime('%H:%M:%S')} - {centername[0]} ※❗タイムアウト：{int(timeout / 1000)}s")
    raise TimeoutError(f"{centername[0]}※❗タイムアウト：{int(timeout / 1000)}s")

#===========v1.6 2026/03/10 Add Start
# ========== Google Calendar Service ==========
def get_calendar_service():
    SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
    if service_account_json:
        service_account_info = json.loads(service_account_json)
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
    elif os.path.exists("service-account.json"):
        creds = service_account.Credentials.from_service_account_file(
            "service-account.json", scopes=SCOPES
        )
    else:
        return None
    return build("calendar", "v3", credentials=creds)

def get_day_reservations(service, target_dates: list[date]) -> list[str]:

    if not service or not target_dates:
        return []

    WEEKDAYS_JP = ["（月）", "（火）", "（水）", "（木）", "（金）", "（土）", "（日）"]

    unique_dates = sorted(set(target_dates))
    tz = ZoneInfo("Asia/Tokyo")
    time_min = datetime(unique_dates[0].year, unique_dates[0].month, unique_dates[0].day,
                        0, 0, 0, tzinfo=tz).isoformat()
    time_max = datetime(unique_dates[-1].year, unique_dates[-1].month, unique_dates[-1].day,
                        23, 59, 59, tzinfo=tz).isoformat()

    events_by_date = defaultdict(list)

    for i, calendar_id in enumerate(calendars):
        booking_type = "予約" if i == 0 else "抽選"
        try:
            result = service.events().list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime"
            ).execute()
            for event in result.get("items", []):
                start_dt = event["start"].get("dateTime")
                if not start_dt:
                    continue
                    
                dt = datetime.fromisoformat(start_dt)
                d = dt.date()
                if d in unique_dates:
                    time_str = dt.strftime("%H:%M")
                    summary = event.get("summary", "")
                    events_by_date[d].append((time_str, summary, booking_type))
        except Exception as e:
            print(f"⚠️ Calendar読み取りエラー: {e}")

    if not events_by_date:
        return []

    lines = []
    lines.append(email_config["line1"])
    first_block = True
    for d in unique_dates:
        if d not in events_by_date:
            continue
        if not first_block:
            lines.append("")   # 日付ブロック間に空行
        weekday_str = WEEKDAYS_JP[d.weekday()]
        lines.append(f"【{d.month}月{d.day}日{weekday_str}】")
        for time_str, summary, booking_type in sorted(events_by_date[d]):
            lines.append(f"・{time_str} {summary}（{booking_type}）")
        first_block = False
    lines.append(email_config["line0"])
    return lines

def get_month_count_summary(service, target_months: list[tuple[int, int]]) -> list[str]:

    if not service or not count_calendar_id or not target_months:
        return []

    tz = ZoneInfo("Asia/Tokyo")
    found_any = False
    lines = []
    lines.append(email_config["line2"])

    first_block = True
    for year, month in sorted(set(target_months)):
        
        next_month = month % 12 + 1
        next_year = year + 1 if month == 12 else year
        time_min = datetime(year, month, 1, tzinfo=tz).isoformat()
        time_max = datetime(next_year, next_month, 1, 0, 0, 0, tzinfo=tz).isoformat()

        try:
            result = service.events().list(
                calendarId=count_calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
            ).execute()

            for event in result.get("items", []):
                
                if "date" not in event.get("start", {}):
                    continue
                desc = event.get("description", "").strip()
                if desc:
                    if not first_block:
                        lines.append("")  
                    for line in desc.splitlines():
                        lines.append(line)
                    found_any = True
                    first_block = False

        except Exception as e:
            print(f"⚠️ 統計Calendar読み取りエラー ({year}/{month}): {e}")

    if not found_any:
        return []

    lines.append(email_config["line0"])
    return lines

def read_calendar_info(body_lines) -> list[str]:
    
    body_lines.append(email_config["line0"])
    try:
        cal_service = get_calendar_service()
        if cal_service:
            target_dates = set()
            today_obj = datetime.now(ZoneInfo("Asia/Tokyo")).date()
            for line in body_lines:
                try:
                    d = extract_date(line)
                    if d >= today_obj:
                        target_dates.add(d)
                except:
                    pass

            # 当日
            reservation_lines = get_day_reservations(cal_service, list(target_dates))
            body_lines.extend(reservation_lines)

            # 統計
            target_months = list({(d.year, d.month) for d in target_dates})
            count_lines = get_month_count_summary(cal_service, target_months)
            body_lines.extend(count_lines)

            body_lines.append(email_config["avaliable"])

    except Exception as e:
        print(f"⚠️ エラー: {e}")

    return body_lines
#===========v1.6 2026/03/10 Add End

#===========v1.7 2026/03/17 Add Start
#===========v2.0 2026/04/08 Upd Start
def get_today_schedule() -> list[str]:
    
    cal_service = get_calendar_service()    
    if not cal_service:
        return []

    tz = ZoneInfo("Asia/Tokyo")
    today = datetime.now(tz).date()
    tomorrow = today + timedelta(days=1)
    
    WEEKDAYS_JP = ["（月）", "（火）", "（水）", "（木）", "（金）", "（土）", "（日）"]
    day_lines = get_day_reservations(cal_service, [today, tomorrow])

    # day_linesをdate別に分解
    events_by_label = {}  # "今日" or "明日" -> list of lines
    current_label = None
    for line in day_lines:
        if line == email_config["line1"] or line == email_config["line0"]:
            continue
        # 【X月X日（X）】形式の行を検出
        m = re.match(r"^【(\d+)月(\d+)日[（(].+[）)]】$", line)
        if m:
            month, day = int(m.group(1)), int(m.group(2))
            d = date(today.year if not (today.month in [11,12] and month in [1,2]) else today.year+1, month, day)
            if d == today:
                current_label = "今日"
            elif d == tomorrow:
                current_label = "明日"
            else:
                current_label = None
            if current_label:
                events_by_label[current_label] = []
        elif current_label and line.startswith("・"):
            events_by_label[current_label].append(line)

    lines = []
    for d, label in [(today, "今日"), (tomorrow, "明日")]:
        weekday_str = WEEKDAYS_JP[d.weekday()]
        lines.append(f"【{d.month}月{d.day}日{weekday_str}】　{label}")
        day_events = events_by_label.get(label, [])
        if day_events:
            lines.extend(day_events)
        else:
            lines.append("・なし")
        lines.append("")  # 日付間の空行

    # 末尾の空行を除去
    if lines and lines[-1] == "":
        lines.pop()

    lines.append(email_config["line0"])
    return lines
#===========v2.0 2026/04/08 Upd End

def merge_body_lines(body_lines: list[str]) -> list[str]:
    merged = {}
    order = []
    current_name = None

    for line in body_lines:
        stripped = line.strip()
        if stripped.startswith("【") and stripped.endswith("】"):
            name = stripped
            if name not in merged:
                merged[name] = []
                order.append(name)
            current_name = name
        elif current_name is not None and line.startswith("・"):
            merged[current_name].append(line)
        else:
            current_name = None

    result = []
    for name in order:
        #===========v1.7 2026/03/17 Upd Start
        if result:  # 不是第一个才加空行
            result.append("")
        result.append(f"【{name[1:-1]}】")
        #===========v1.7 2026/03/17 Upd End
        result.extend(merged[name])
    return result
#===========v1.7 2026/03/17 Add End

#===========v2.2 2026/04/13 Add Start
NOMAIL_FILE = os.path.join("input", "nomail.txt")

def load_nomail_lines() -> set[str]:
    """nomail.txtから施設略称と・行の組合せキーをsetで返す（例: "N.|・5月10日..."）"""
    if not os.path.exists(NOMAIL_FILE):
        return set()
    result = set()
    current_prefix = ""
    with open(NOMAIL_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            m = re.match(r"^【([A-Z]+\.).*?】$", line)
            if m:
                current_prefix = m.group(1)
            elif line.startswith("・") and current_prefix:
                result.add(f"{current_prefix}|{line}")
    return result

def cleanup_nomail(today: date):
    """nomail.txtから当日以前の日付を含む行と、その後空になったブロックを削除する"""
    if not os.path.exists(NOMAIL_FILE):
        return
    with open(NOMAIL_FILE, encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]

    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # 施設ブロックのヘッダー行
        if re.match(r"^【[A-Z]\..*?】$", line):
            header = line
            block_lines = []
            i += 1
            while i < len(lines) and not re.match(r"^【[A-Z]\..*?】$", lines[i]):
                block_lines.append(lines[i])
                i += 1
            # ・行のうち、当日以降のものだけ残す
            kept = []
            for bl in block_lines:
                if not bl.startswith("・"):
                    kept.append(bl)
                    continue
                try:
                    d = extract_date(bl)
                    if d >= today:
                        kept.append(bl)
                except:
                    kept.append(bl)
            # ・行が1行以上残っていればブロックを維持
            has_slot = any(bl.startswith("・") for bl in kept)
            if has_slot:
                new_lines.append(header)
                new_lines.extend(kept)
        else:
            new_lines.append(line)
            i += 1

    # 末尾の空行を整理
    while new_lines and new_lines[-1].strip() == "":
        new_lines.pop()

    with open(NOMAIL_FILE, "w", encoding="utf-8") as f:
        for line in new_lines:
            f.write(line + "\n")
#===========v2.2 2026/04/13 Add End

async def main(f=None):
    # 開始
    start = datetime.now(ZoneInfo("Asia/Tokyo"))
    # start = datetime.now()
    
    #===========v2.2 2026/04/13 Add Start
    cleanup_nomail(start.date())
    #===========v2.2 2026/04/13 Add End
    
    async with async_playwright() as playwright:

        tasks_scc = [
            process_kaikan(playwright, kaikan, kaikan21, kaikan22, _, page, label, name, index)
            for index, (kaikan, kaikan21, kaikan22, _, page, label, name) in enumerate(SCC)
            if kaikan != "000000"
        ]

        results = await asyncio.gather(*tasks_scc, return_exceptions=True)

    # 合并结果
    body_lines = []
    errorflag = ""
    for group in results:
        if isinstance(group, Exception):
            errorflag = "X"
            print(f"⚠️ 某个任务失败: {type(group).__name__} - {group}")
            continue
        body_lines.extend(group)

    #===========v2.0 2026/04/08 Upd Start
    body_lines = merge_body_lines(body_lines)
    #===========v2.0 2026/04/08 Upd End
    
    # 错误判断
    sent = ''    
    if errorflag == "" or ( errorflag != "" and body_lines ):  

        # 保存文件
        timestamp = get_timestamp()
        file_new = os.path.join(OUTPUT_DIR, f"{timestamp}.txt")
        print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - ファイル保存 {file_new}")

        #===========v2.0 2026/04/08 Upd Start
        file_content = [
            re.sub(r"^【([A-Z]+\.).*?】$", r"【\1】", line) if re.match(r"^【[A-Z]+\..*?】$", line) else line
            for line in body_lines
        ]
        save_file(file_content, file_new)

        # 差分比较
        files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".txt"))
        if len(files) >= 2:
            file_prev1 = os.path.join(OUTPUT_DIR, files[-3])
            if compare_files(file_prev1, file_new):

                # 読取上一次和本次内容
                with open(file_prev1, encoding="utf-8") as f:
                    prev1_raw = f.read().splitlines()
                with open(file_new, encoding="utf-8") as f:
                    new_raw = f.read().splitlines()

                def make_keyed_set(raw_lines):
                    """各・行に施設略称プレフィックスを付けたsetを返す"""
                    keyed = set()
                    prefix = ""
                    for line in raw_lines:
                        m = re.match(r"^【([A-Z]+\.).*?】$", line.strip())
                        if m:
                            prefix = m.group(1)
                        elif line.startswith("・") and prefix:
                            keyed.add(f"{prefix}|{line}")
                        else:
                            keyed.add(line)  # 空行・ヘッダ行はそのまま
                    return keyed

                prev1_keyed = make_keyed_set(prev1_raw)
                new_keyed   = make_keyed_set(new_raw)

                prev1_lines = set(prev1_raw)
                new_lines   = set(new_raw)

                added   = new_lines - prev1_lines
                removed = prev1_lines - new_lines

                added_keyed   = new_keyed - prev1_keyed
                removed_keyed = prev1_keyed - new_keyed

                meaningful_added = {
                    line for line in added
                    if line.strip() and not line.startswith("【")
                }
                
                meaningful_removed = [
                    line for line in removed
                    if line.strip() and not line.startswith("【")
                ]

                meaningful_added_keyed = {
                    k for k in added_keyed
                    if "|" in k  # 施設付き・行のみ
                }
                meaningful_removed_keyed = [
                    k for k in removed_keyed
                    if "|" in k
                ]

                #===========v2.2 2026/04/13 Add Start
                # nomail.txtに基づくスキップ判定（施設名付きキーで比較）
                nomail_lines = load_nomail_lines()
                # 新規追加のうちnomail外の行があれば送信対象
                added_outside_nomail = meaningful_added_keyed - nomail_lines
                # 削除のうちnomail外の行があれば送信対象
                removed_outside_nomail = [l for l in meaningful_removed_keyed if l not in nomail_lines]
                # 変化がすべてnomail内に収まる場合はスキップ
                all_changes_in_nomail = (
                    bool(nomail_lines)
                    and not added_outside_nomail
                    and not removed_outside_nomail
                    and (bool(meaningful_added_keyed) or bool(meaningful_removed_keyed))
                )
                #===========v2.2 2026/04/13 Add End

                should_send = bool(added_outside_nomail or removed_outside_nomail)
                if should_send:
                    direction = []
                    if added_outside_nomail:
                        direction.append("追加あり")
                    if removed_outside_nomail:
                        direction.append("削除あり")
                    print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - ファイル比較\n           新 {file_new}\n           旧 {file_prev1}\n           差異あり（{'、'.join(direction)}）、Calendar読込✅")
                    has_avali = bool(body_lines)
                    if body_lines:
                        body_lines = read_calendar_info(body_lines)
                    print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - メール送信✅")
                    send_mail(body_lines, has_avali)
                    sent = 'X'
                else:
                    skip_reason = []
                    if all_changes_in_nomail:
                        direction = []
                        if meaningful_added_keyed:
                            direction.append("追加あり")
                        if meaningful_removed_keyed:
                            direction.append("削除あり")
                        skip_reason.append(f"nomail対象のみの変化（{'、'.join(direction)}）")
                    elif not meaningful_added and not meaningful_removed:
                        skip_reason.append("意味のある差異なし（ヘッダ行・空行のみの変化）")
                    reason_str = "、".join(skip_reason) if skip_reason else "不明"
                    print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - ファイル比較\n           新 {file_new}\n           旧 {file_prev1}\n           差異あり但送信スキップ（{reason_str}）🔕")
                    
                    #----後日削除Start
                    has_avali = bool(body_lines)
                    send_mail(body_lines, has_avali)
                    sent = 'X'
                    #----後日削除End

            
            else:
                print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - ファイル比較\n           新 {file_new}\n           旧 {file_prev1}\n           差異なし、送信不要🔕")

        else:
            print("旧ファイル存在なし、メール送信")
            send_mail(body_lines, bool(body_lines))
            sent = 'X'
        #===========v2.0 2026/04/08 Upd End
                
    else:
        files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".txt"))

    #===========v1.7 2026/03/17 Upd Start
    if start.hour < 1 and sent == '':
        today_str = start.strftime("%Y%m%d")
        sent_flag_file = os.path.join(OUTPUT_DIR, "daily_sent.txt")
        last_sent_date = ""
        if os.path.exists(sent_flag_file):
            with open(sent_flag_file, "r") as f:
                last_sent_date = f.read().strip()
        if last_sent_date != today_str:
            has_avali = bool(body_lines)
            if body_lines:
                body_lines = read_calendar_info(body_lines)
            print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - 0時強制送信✅")
            send_mail(body_lines, has_avali)
        else:
            print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - 0時送信済み、スキップ🔕")
    #===========v1.7 2026/03/17 Upd End

    # 清理旧文件
    if start.minute < 10 and len(files) > 6:
        for file in files[:-6]:
            try:
                os.remove(os.path.join(OUTPUT_DIR, file))
                print(f"ファイル削除 {file}")
            except Exception as e:
                print(f"⚠️ 削除失敗: {file} - {type(e).__name__}: {e}")

    # 结束
    end = datetime.now(ZoneInfo("Asia/Tokyo"))
    # end = datetime.now()
    duration = int((end - start).total_seconds())
    minutes, seconds = divmod(duration, 60)
    print(f"{end.strftime('%H:%M:%S')} - 処理終了　※処理時間：{minutes}m {seconds}s")

async def process_kaikan(playwright, kaikan, kaikan21, kaikan22, _, page_lc, label, name, index) -> list[str]:
    start = datetime.now(ZoneInfo("Asia/Tokyo"))
    # start = datetime.now()

    #===========v2.0 2026/04/08 Add Start
    browser = None
    try:
    #===========v2.0 2026/04/08 Add End
        
        browser = await playwright.chromium.launch(headless=True)
        # browser = await playwright.chromium.launch(headless=False)
        page = await browser.new_page()
        
        await page.goto(email_config["link"])
        # await page.goto("xxx")
    
        # await page.wait_for_selector("iframe[name='MainFrame']", timeout=30000)
        await page.wait_for_load_state("domcontentloaded")
        frame = page.frame(name="MainFrame")
        if not frame:
            print(f"❌ MainFrame not found for {name[0]}")
            await browser.close()
            return []
    
        await frame.wait_for_selector("input[alt='目的']", timeout=30000)
        await frame.locator("input[alt='目的']").click()
    
        checkbox_selector = f"input[name='chk_bunrui1_{kaikan}']"
        await frame.wait_for_selector(checkbox_selector, timeout=30000)
        await frame.locator(checkbox_selector).check()
    
        await frame.locator(f'input[alt="{web_ele["noloca"]}"]').click()
    
        if page_lc != "0":
            await frame.wait_for_selector(f'input[alt="{web_ele["nextpage"]}"]', timeout=30000)
            await frame.locator(f'input[alt="{web_ele["nextpage"]}"]').click()
    
        await frame.wait_for_selector(f"input[onclick*=\"cmdYoyaku_click('{kaikan21}','{kaikan22}')\"]", timeout=30000)
        await frame.locator(f"input[onclick*=\"cmdYoyaku_click('{kaikan21}','{kaikan22}')\"]").click()
    
        await frame.wait_for_selector('input[name="disp_mode"]', timeout=10000)
        page.on("dialog", lambda dialog: dialog.accept())  # 全局弹窗处理保险
        old_html = await frame.locator("table.clsKoma").first.inner_html()
    
        try:
            async with page.expect_event("dialog", timeout=5000):
                await frame.locator("input[name='disp_mode'][value='0']").click()
    
        except Exception as e:
            print(f"⚠️ {name[0]} - 弹窗处理失败: {e}")
    
        result = []
    
        # 本日～未来4週間
        previs = 0
        kaikan = 0
        for row in SCC:
            _, kaikan21_lc, _, shisetu_lc, _, label_lc, name_lc = row
            if label_lc == label:
                lines, old_html = await process_shisetu(_, kaikan21_lc, _, shisetu_lc, _, _, name_lc, frame, old_html, previs, kaikan)
                result.append(lines)
                previs += 1
                kaikan += 1
    
        await frame.locator(f'img[alt="{web_ele["nextweek"]}"]').first.click()
    
        # 未来4週間～未来8週間
        previs = 0
        kaikan = 1
        for row in SCC:
            _, kaikan21_lc, _, shisetu_lc, _, label_lc, name_lc = row
            if label_lc == label:
                lines, old_html = await process_shisetu(_, kaikan21_lc, _, shisetu_lc, _, _, name_lc, frame, old_html, previs, kaikan)
                result.append(lines)
                previs += 1
                kaikan += 1
    
    #===========v1.4 2025/12/29 Add Start
        today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
        date_8_weeks_later = get_date_n_weeks_later(today, 8)
        end_of_month_after_next = get_end_of_month_after_next(today)
        
        if date_8_weeks_later < end_of_month_after_next:
            
            # 未来9週間～未来10週間
            await frame.locator(f'img[alt="{web_ele["nextweek"]}"]').first.click()
            
            previs = 0
            kaikan = 2
            for row in SCC:
                _, kaikan21_lc, _, shisetu_lc, _, label_lc, name_lc = row
                if label_lc == label:
                    lines, old_html = await process_shisetu(_, kaikan21_lc, _, shisetu_lc, _, _, name_lc, frame, old_html, previs, kaikan)
                    result.append(lines)
                    previs += 1
                    kaikan += 1
    #===========v1.4 2025/12/29 Add End
        
        await browser.close()
        body_line = [line for group in result for line in group]
        return body_line
      
    #===========v2.0 2026/04/08 Add Start
    except Exception as e:
        print(f"⚠️ {name[0]} 処理失敗: {type(e).__name__} - {e}")
        if browser:
            try:
                await browser.close()
            except:
                pass
        return []
    #===========v2.0 2026/04/08 Add End  

async def process_shisetu(_, kaikan21_lc, __, shisetu, ___, ____, name, frame: Frame, old_html: str, previs: int, kaikan: int) -> tuple[list[str], str]:

    start = datetime.now(ZoneInfo("Asia/Tokyo"))
    # start = datetime.now()

    body_lines_lc = []
    old_html_lc = old_html

    try:
        if previs != 0:
            await frame.locator(f'img[alt="{web_ele["preweek"]}"]').first.click()

        if kaikan != 0 and "センター" not in name and "中央" not in name:
            new_html = await wait_for_html_change(frame, "table.clsKoma", old_html_lc, name)
            old_html_lc = new_html
            await frame.wait_for_timeout(2000)
            await frame.select_option("select[name='lst_kaikan']", value=kaikan21_lc)
        
        if kaikan == 0 and shisetu != "000":
            new_html = await wait_for_html_change(frame, "table.clsKoma", old_html_lc, name)
            old_html_lc = new_html
            await frame.select_option("select[name='lst_shisetu']", value=shisetu)

        new_html = await wait_for_html_change(frame, "table.clsKoma", old_html_lc, name)
        old_html_lc = new_html
        date_to_times = await get_avalinfo(frame)

        await frame.locator(f'img[alt="{web_ele["nextweek"]}"]').first.click()
        new_html = await wait_for_html_change(frame, "table.clsKoma", old_html_lc, name)
        old_html_lc = new_html
        date_to_times.update(await get_avalinfo(frame))

    except TimeoutError:
        #===========v2.0 2026/04/08 Upd Start
        return [], old_html_lc
        #===========v2.0 2026/04/08 Upd End

    if date_to_times:
        body_lines_lc.append(f"\n【{name}】")
        for date, times in date_to_times.items():
            line = f"・{date} - " + "、".join(times)
            body_lines_lc.append(line)

    end = datetime.now(ZoneInfo("Asia/Tokyo"))
    # end = datetime.now()
    print(f"{start.strftime('%H:%M:%S')} - {name[0]} 　※処理時間：{int((end - start).total_seconds())}s")

    return body_lines_lc, old_html_lc

async def get_avalinfo(frame: Frame) -> dict:

    avalinfo = defaultdict(list)

    day_map = {}
    for th in await frame.locator("th[id^='Day_']").all():
        day_id = await th.get_attribute("id")
        day_text = await th.inner_text()
        if day_id and day_text:
            day_map[day_id.replace("Day_", "")] = day_text.strip()

    icons = await frame.locator(
        "img[alt='予約可能'][src='../image/s_empty.gif'], img[alt='予約可能'][src='../image/s_empty4.gif']"
    ).all()

    today = datetime.now(ZoneInfo("Asia/Tokyo"))
    
#===========v1.4 2025/12/29 Add Start
    end_of_this_month = calendar.monthrange(today.year, today.month)[1] # 今月末

#===========v1.5 2026/01/29 Upd Start
    # 今月末17時以降
    if today.day == end_of_this_month and today.hour >= 17:  # 月末（例：1月31号 17時）
#===========v1.5 2026/01/29 Upd End
        deadline = get_end_of_month_after_next(today) # 翌々月末まで
    else:
        deadline = get_end_of_next_month(today)  # 翌月末まで
#===========v1.4 2025/12/29 Add End
    
    for icon in icons:
        parent_a = await icon.evaluate_handle("el => el.parentElement")
        href = await parent_a.get_property("href")
        href_str = await href.json_value()

        if not isinstance(href_str, str):
            continue

        match = re.search(r'komaClicked\((\d+),(\d+),(\d+)\)', href_str)
        if not match:
            continue

        day_idx, row, col = match.groups()
        date_text = day_map.get(day_idx)
        if not date_text:
            continue
            
#===========v1.4 2025/12/29 Upd Start
        target_date = extract_date(date_text)
        if target_date > deadline:
            return avalinfo
#===========v1.4 2025/12/29 Upd End
            
        holiday = ""
        match = re.search(r"(\d{1,2})月(\d{1,2})日", date_text)
        if match:
            month, day = int(match.group(1)), int(match.group(2))            
            if today.month in [11, 12] and month in [1, 2]:
                year = today.year + 1
            else:
                year = today.year
            date_to_check = datetime(year, month, day).date()
            if weekend_or_holiday(date_to_check):
                holiday = "X"

        if holiday == "X" and row in time_slots:
            time = time_slots[row]
        elif row == '5':
            time = '19:00～21:00'
        else:
            time = ""

        if time:
            avalinfo[date_text].append(time)

    return avalinfo

if __name__ == "__main__":
    asyncio.run(main())
