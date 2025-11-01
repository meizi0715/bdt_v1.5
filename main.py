import os
import re
import json
import asyncio
import smtplib
import hashlib
import calendar
import jpholiday
from zoneinfo import ZoneInfo
from datetime import datetime, date
from collections import defaultdict
from email.mime.text import MIMEText
from playwright.async_api import Frame
from playwright.async_api import async_playwright

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

def extract_date(text: str, year: int = None) -> date:
    match = re.search(r"(\d{1,2})月(\d{1,2})日", text)
    if not match:
        raise ValueError("无法从字符串中提取日期")
    month = int(match.group(1))
    day = int(match.group(2))
    if year is None:
        year = datetime.today().year
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
                # 展开列表中的每个元素（即使只有一个）
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

def send_mail(body_lines: list[str]):

    if body_lines:
        email_body = email_config["header"] + body_lines[0] + "\n" + "\n".join(body_lines[1:] + [email_config["footer"]])
    else:
        email_body = "\n".join([email_config["header"]] + [email_config["noavali"]] + [email_config["footer"]])
        
    msg = MIMEText(email_body, "plain", "utf-8")

    today = datetime.now(ZoneInfo("Asia/Tokyo"))
    msg["Subject"] = f"{email_config['subject']}({today.strftime('%m/%d')})"
    msg["From"] = email_config["from"]
    msg["To"] = email_config["to"]
    # msg["From"] = "xxx@gmail.com"
    # msg["To"] = "xxx@gmail.com"

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(email_config["from"], email_config["pass"])
        # server.login("xxx@gmail.com", "xxx")
        server.send_message(msg)

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

async def main(f=None):
    # 開始
    start = datetime.now(ZoneInfo("Asia/Tokyo"))
    # start = datetime.now()

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

    # 错误判断
    sent = ''    
    if errorflag == "" or ( errorflag != "" and body_lines ):  
    
        # 保存文件
        timestamp = get_timestamp()
        file_new = os.path.join(OUTPUT_DIR, f"{timestamp}.txt")
        print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - ファイル保存 {file_new}")
        # print(f"{datetime.now().strftime('%H:%M:%S')} - ファイル保存 {file_new}")
        file_content = [re.sub(r"【([A-Z])\..+?】", r"【\1.】", line) for line in body_lines]
        save_file(file_content, file_new)
    
        # 差分比较
        files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".txt"))
        if len(files) >= 2:
            file_old = os.path.join(OUTPUT_DIR, files[-2])
            if compare_files(file_old, file_new):
                print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - ファイル比較\n           新 {file_new}\n           旧 {file_old}\n           差異あり、メール送信✅")
                # print(f"{datetime.now().strftime('%H:%M:%S')} - ファイル比較\n           新 {file_new}\n           旧 {file_old}\n           差異あり、メール送信✅")        
                send_mail(body_lines)
                sent = 'X'
            else:
                print(f"{datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%H:%M:%S')} - ファイル比較\n           新 {file_new}\n           旧 {file_old}\n           差異なし、送信不要🔕")
                # print(f"{datetime.now().strftime('%H:%M:%S')} - ファイル比較\n           新 {file_new}\n           旧 {file_old}\n           差異なし、送信不要🔕")
    
        else:
            print("旧ファイル存在なし、メール送信")
            send_mail(body_lines)
            sent = 'X'
            
    else:
        files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".txt"))
        
    # 朝0時0分
    if start.hour == 0 and start.minute < 10 and sent == '':
        send_mail(body_lines)

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

    await browser.close()
    body_line = [line for group in result for line in group]
    return body_line

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
        return body_lines_lc, old_html_lc

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
            
        # 翌月末まで
        target_date = extract_date(date_text)
        end_of_next_month = get_end_of_next_month()
        if target_date > end_of_next_month:
            return avalinfo
            
        # holiday = "X"
        holiday = ""
        match = re.search(r"(\d{1,2})月(\d{1,2})日", date_text)
        if match:
            month, day = int(match.group(1)), int(match.group(2))
            date_to_check = datetime(2025, month, day).date()
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
