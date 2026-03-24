import asyncio
import aiohttp
import json
import logging
import os
import time
import html
from typing import Tuple, Any
from datetime import datetime
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ========== НАСТРОЙКА ЛОГИРОВАНИЯ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========
TELEGRAM_TOKEN = "TOKEN"
CONFIG_FILE    = "config.json"

BRANDS = [
    {"name": "Comme des Fuckdown", "url": "https://www.vinted.de/api/v2/catalog/items",
     "params": {"search_text": "comme des fuckdown", "brand_ids[]": 27215, "order": "newest_first", "per_page": 20}},
    {"name": "Racer Worldwide", "url": "https://www.vinted.de/api/v2/catalog/items",
     "params": {"search_text": "racer worldwide", "brand_ids[]": 2810300, "order": "newest_first", "per_page": 20}},
    {"name": "Gosha Rubchinskiy", "url": "https://www.vinted.de/api/v2/catalog/items",
     "params": {"search_text": "gosha rubchinskiy", "brand_ids[]": [219304, 1908821], "order": "newest_first",
                "per_page": 20}},
    {"name": "Alpha Industries", "url": "https://www.vinted.de/api/v2/catalog/items",
     "params": {"search_text": "alpha industries", "brand_ids[]": 60712, "order": "newest_first", "per_page": 20}}
]

STRICT_INTERVALS    = [30, 60, 90]
MAX_RETRY_429       = 3
BASE_RETRY_DELAY    = 60
SENT_ITEMS_TTL_DAYS = 7
SENT_ITEMS_TTL_SEC  = SENT_ITEMS_TTL_DAYS * 86400
MAX_CONSECUTIVE_401 = 2

USERS_FILE    = "users.txt"
COOKIES_FILE  = "cookies.json"
STORAGE_FILES = {b["name"]: f"sent_{b['name'].replace(' ', '_').lower()}.json" for b in BRANDS}

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()


# ========== ИСКЛЮЧЕНИЯ ==========
class CookiesExpiredError(Exception):
    pass

class Vpn403Error(Exception):
    def __init__(self, brand_name: str, url: str):
        self.brand_name = brand_name
        self.url = url
        super().__init__(f"403 для {brand_name}")

class Api404Error(Exception):
    def __init__(self, brand_name: str, url: str):
        self.brand_name = brand_name
        self.url = url
        super().__init__(f"404 для {brand_name}")

class RateLimitError(Exception):
    def __init__(self, brand_name: str, retry_after: int = BASE_RETRY_DELAY):
        self.brand_name  = brand_name
        self.retry_after = retry_after
        super().__init__(f"429 для {brand_name}")

class ContentTypeError(Exception):
    def __init__(self, brand_name: str, url: str, content_type: str):
        self.brand_name   = brand_name
        self.url          = url
        self.content_type = content_type
        super().__init__(f"Неожиданный Content-Type для {brand_name}")


# ========== СОБЫТИЯ И СОСТОЯНИЯ ==========
cookies_ready_event = asyncio.Event()
cookies_ready_event.set()

cookie_refresh_active    = False
cookie_refresh_stage     = 0
cookie_temp_session      = None
cookie_temp_access_token = None
cookie_ask_user_id       = None
cookie_refresh_lock      = asyncio.Lock()
consecutive_401_count    = 0
auto_refresh_in_progress = False


# ========== КОНФИГ ==========
def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"⚠️ {CONFIG_FILE} не найден — авто-обновление куки недоступно")
        return {}
    except Exception:
        logger.exception(f"💥 Ошибка чтения {CONFIG_FILE}")
        return {}


# ========== PLAYWRIGHT: АВТО-ОБНОВЛЕНИЕ КУКИ ==========
async def auto_refresh_cookies() -> bool:
    global auto_refresh_in_progress

    if auto_refresh_in_progress:
        logger.info("⏳ Авто-обновление уже идёт, пропускаю")
        return False

    config   = load_config()
    email    = config.get("vinted_email", "")
    password = config.get("vinted_password", "")

    if not email or not password:
        logger.warning("⚠️ Email/password не заданы в config.json")
        return False

    auto_refresh_in_progress = True
    logger.info("🌐 Запускаю Playwright...")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                ]
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="de-DE",
                timezone_id="Europe/Berlin",
                viewport={"width": 1280, "height": 800},
                java_script_enabled=True,
                extra_http_headers={
                    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                }
            )
            page = await context.new_page()

            # ── Шаг 1: Открываем главную страницу ────────────────────────
            logger.info("🔗 Открываю главную vinted.de...")
            await page.goto(
                "https://www.vinted.de",
                timeout=60000
            )
            await page.wait_for_load_state("networkidle", timeout=40000)
            await asyncio.sleep(3)

            current_url = page.url
            logger.info(f"📍 URL: {current_url}")

            # ── Шаг 2: Выбираем страну "Deutschland" ──────────────────────
            logger.info("🌍 Проверяю модальное окно выбора страны...")
            DEUTSCHLAND_SELECTORS = [
                "text=Deutschland",
                "li:has-text('Deutschland')",
                "button:has-text('Deutschland')",
                "a:has-text('Deutschland')",
                "[href*='vinted.de']:has-text('Deutschland')",
            ]
            for sel in DEUTSCHLAND_SELECTORS:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.locator(sel).first.click()
                        logger.info(f"✅ Выбрал Deutschland через: {sel}")
                        await asyncio.sleep(3)
                        break
                except Exception:
                    continue

            # ── Шаг 3: Принимаем cookie-баннер ───────────────────────────
            logger.info("🍪 Ищу cookie-баннер...")
            await asyncio.sleep(2)
            COOKIE_SELECTORS = [
                "text=Alle zulassen",
                "button:has-text('Alle zulassen')",
                "#onetrust-accept-btn-handler",
                "button[id*='accept']",
                "button:has-text('Accept')",
                "button:has-text('Akzeptieren')",
                "[aria-label*='Akzeptieren']",
                "[aria-label*='Accept']",
            ]
            for sel in COOKIE_SELECTORS:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.locator(sel).first.click()
                        await asyncio.sleep(2)
                        logger.info(f"🍪 Cookie-баннер принят: {sel}")
                        break
                except Exception:
                    continue

            # ── Шаг 4: Кликаем "Einloggen" в хедере ─────────────────────
            logger.info("🖱️ Ищу кнопку входа в хедере...")
            await asyncio.sleep(1)

            # Логируем все ссылки для отладки
            all_links = await page.locator("a").all()
            logger.info(f"🔗 Ссылок на странице: {len(all_links)}")
            for i, link in enumerate(all_links[:30]):
                try:
                    info = await link.evaluate(
                        "el => ({href: el.href, text: el.innerText.trim()})"
                    )
                    if info.get("text"):  # Только ссылки с текстом
                        logger.info(f"   a[{i}]: {info}")
                except Exception:
                    pass

            HEADER_LOGIN_SELECTORS = [
                "a:has-text('Einloggen')",
                "button:has-text('Einloggen')",
                "a[href*='signup']",
                "a[href*='register']",
                "[data-testid*='login']",
                "[data-testid*='header']",
            ]
            header_clicked = False
            for sel in HEADER_LOGIN_SELECTORS:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.locator(sel).first.click()
                        logger.info(f"✅ Кнопка входа нажата: {sel}")
                        header_clicked = True
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            if not header_clicked:
                logger.error("❌ Кнопка входа в хедере не найдена!")
                await page.screenshot(path="debug_login_error.png")
                with open("debug_login_page.html", "w", encoding="utf-8") as f:
                    f.write(await page.content())
                await browser.close()
                return False

            # ── Шаг 5: Нажимаем "Einloggen" в модалке/форме ──────────────
            logger.info("🖱️ Ищу ссылку 'Einloggen' внутри формы...")
            await asyncio.sleep(2)
            EINLOGGEN_SELECTORS = [
                "a:has-text('Einloggen')",
                "button:has-text('Einloggen')",
                "text=Einloggen",
            ]
            for sel in EINLOGGEN_SELECTORS:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.locator(sel).first.click()
                        logger.info(f"✅ 'Einloggen' нажат: {sel}")
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            # ── Шаг 6: Нажимаем ссылку "E-Mail" ─────────────────────────
            logger.info("🖱️ Ищу ссылку 'E-Mail'...")
            await asyncio.sleep(2)
            EMAIL_LINK_SELECTORS = [
                "a:has-text('E-Mail')",
                "text=E-Mail",
                "a[href*='email']",
            ]
            for sel in EMAIL_LINK_SELECTORS:
                try:
                    if await page.locator(sel).count() > 0:
                        await page.locator(sel).first.click()
                        logger.info(f"✅ Ссылка 'E-Mail' нажата: {sel}")
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            # ── Шаг 7: Ждём форму входа ───────────────────────────────────
            logger.info("🖱️ Нажимаю кнопку входа...")
            SUBMIT_SELECTORS = [
                '[data-testid="login-form--submit-button"]',
                '[data-testid*="submit"]',
                "button:has-text('Einloggen')",
                "button:has-text('Anmelden')",
                "button:has-text('Log in')",
                'button[type="submit"]',
                'input[type="submit"]',
            ]
            submit_ok = False
            for sel in SUBMIT_SELECTORS:
                try:
                    if await page.locator(sel).count() > 0:
                        locator = page.locator(sel).first
                        await locator.wait_for(state="visible", timeout=3000)
                        await locator.click()
                        logger.info(f"✅ Submit нажат через: {sel}")
                        submit_ok = True
                        break
                except Exception:
                    continue

            if not submit_ok:
                logger.error("❌ Кнопка submit не найдена!")
                await page.screenshot(path="debug_login_error.png")
                await browser.close()
                return False

            # ── Шаг 8: Ждём куки ─────────────────────────────────────────
            logger.info("⏳ Жду куки после входа...")
            try:
                await page.wait_for_function(
                    """() => document.cookie.includes('access_token_web') &&
                             (document.cookie.includes('__vinted_fr_session') ||
                              document.cookie.includes('_vinted_fr_session'))""",
                    timeout=15000
                )
                logger.info("✅ Куки появились!")
            except PlaywrightTimeoutError:
                logger.warning(f"⚠️ Куки не появились за 15 сек. URL: {page.url}")

            await asyncio.sleep(2)

            # ── Шаг 9: Забираем куки ─────────────────────────────────────
            cookies      = await context.cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies}
            logger.info(f"🍪 Куки: {list(cookies_dict.keys())}")

            # Берём session — пробуем оба варианта имени
            session_val = (
                cookies_dict.get("__vinted_fr_session") or
                cookies_dict.get("_vinted_fr_session")
            )
            token_val = cookies_dict.get("access_token_web")

            # Fallback — localStorage
            if not token_val:
                try:
                    token_val = await page.evaluate(
                        "() => localStorage.getItem('access_token_web') "
                        "|| localStorage.getItem('access_token') "
                        "|| sessionStorage.getItem('access_token_web')"
                    )
                    if token_val:
                        logger.info("✅ Токен найден в localStorage")
                except Exception:
                    pass

            await browser.close()

            # ── Шаг 10: Сохраняем ────────────────────────────────────────
            if session_val and token_val:
                existing = load_cookies()
                existing["__vinted_fr_session"] = session_val
                existing["access_token_web"]    = token_val
                with open(COOKIES_FILE, "w") as f:
                    json.dump(existing, f, ensure_ascii=False)
                logger.info("✅ Куки успешно сохранены!")
                return True
            else:
                logger.error(
                    f"❌ Куки не получены.\n"
                    f"   session = {'ДА' if session_val else 'НЕТ'}\n"
                    f"   token   = {'ДА' if token_val else 'НЕТ'}\n"
                    f"   Проверь debug_after_login.png"
                )
                return False

    except PlaywrightTimeoutError as e:
        logger.error(f"❌ Playwright timeout: {e}")
        return False
    except Exception:
        logger.exception("💥 Ошибка Playwright")
        return False
    finally:
        auto_refresh_in_progress = False


# ========== БЕЗОПАСНАЯ ОТПРАВКА ==========
async def safe_send_message(user_id: int, text: str, parse_mode: str = "HTML", max_retries: int = 3) -> bool:
    for attempt in range(max_retries):
        try:
            await bot.send_message(int(user_id), text, parse_mode=parse_mode)
            return True
        except TelegramRetryAfter as e:
            wait = e.retry_after + 1
            logger.warning(f"⏳ Flood control для {user_id}, жду {wait} сек (попытка {attempt + 1}/{max_retries})")
            await asyncio.sleep(wait)
        except TelegramForbiddenError:
            logger.warning(f"🚫 {user_id} заблокировал бота, удаляю")
            remove_user(user_id)
            return False
        except TelegramBadRequest as e:
            logger.error(f"❌ BadRequest для {user_id}: {e}")
            return False
        except Exception as e:
            logger.exception(f"💥 Ошибка отправки для {user_id}: {e}")
            return False
    logger.error(f"❌ Не удалось отправить {user_id} после {max_retries} попыток")
    return False


async def safe_send_photo(user_id: int, photo: str, caption: str, parse_mode: str = "HTML", max_retries: int = 3) -> bool:
    for attempt in range(max_retries):
        try:
            await bot.send_photo(int(user_id), photo, caption=caption, parse_mode=parse_mode)
            return True
        except TelegramRetryAfter as e:
            wait = e.retry_after + 1
            logger.warning(f"⏳ Flood control фото для {user_id}, жду {wait} сек (попытка {attempt + 1}/{max_retries})")
            await asyncio.sleep(wait)
        except TelegramForbiddenError:
            logger.warning(f"🚫 {user_id} заблокировал бота, удаляю")
            remove_user(user_id)
            return False
        except TelegramBadRequest as e:
            logger.error(f"❌ BadRequest фото для {user_id}: {e}")
            return False
        except Exception as e:
            logger.exception(f"💥 Ошибка отправки фото для {user_id}: {e}")
            return False
    return False


# ========== УВЕДОМЛЕНИЯ ==========
async def notify_all_users(text: str, parse_mode: str = "HTML"):
    users = load_users()
    for user_id in users:
        await safe_send_message(int(user_id), text, parse_mode=parse_mode)
        await asyncio.sleep(0.05)


async def notify_cookies_refresh_needed():
    global cookie_refresh_active, cookie_refresh_stage, cookie_temp_session, \
        cookie_temp_access_token, cookie_ask_user_id

    async with cookie_refresh_lock:
        if cookie_refresh_active:
            return
        cookie_refresh_active    = True
        cookie_refresh_stage     = 0
        cookie_temp_session      = None
        cookie_temp_access_token = None
        cookie_ask_user_id       = None
        cookies_ready_event.clear()

        users = list(load_users())
        if users:
            for user_id in users:
                await safe_send_message(
                    int(user_id),
                    "🔐 <b>Требуется ручной ввод куки.</b>\n\n"
                    "Пришли значение "
                    "<code>__vinted_fr_session</code> (одной строкой).",
                )
        else:
            logger.warning("⚠️ users.txt пуст — некому отправить запрос куки")


async def notify_429(brand_name: str, attempt: int, wait: int):
    await notify_all_users(
        f"⚠️ <b>Ошибка 429 — Too Many Requests</b>\n\n"
        f"🏷 Бренд: <b>{html.escape(brand_name)}</b>\n"
        f"🔁 Попытка: {attempt}/{MAX_RETRY_429}\n"
        f"⏳ Жду <b>{wait} сек</b> перед повтором...\n\n"
        f"Vinted временно ограничил запросы. Бот продолжит работу."
    )


async def notify_429_fatal(brand_name: str):
    await notify_all_users(
        f"🚨 <b>Критическая ошибка 429</b>\n\n"
        f"🏷 Бренд: <b>{html.escape(brand_name)}</b>\n\n"
        f"❌ Все {MAX_RETRY_429} попытки исчерпаны.\n\n"
        f"📋 Возможные причины:\n"
        f"• Слишком частые запросы\n"
        f"• IP в бан-листе Vinted\n"
        f"• Требуется смена IP/VPN\n\n"
        f"🛑 <b>Бот остановлен.</b>"
    )


async def notify_content_type_error(brand_name: str, url: str, content_type: str):
    await notify_all_users(
        f"🚨 <b>Неожиданный ответ от Vinted</b>\n\n"
        f"🏷 Бренд: <b>{html.escape(brand_name)}</b>\n"
        f"🔗 URL: <code>{html.escape(url)}</code>\n"
        f"📄 Тип ответа: <code>{html.escape(content_type)}</code>\n\n"
        f"📋 Возможные причины:\n"
        f"• Vinted показывает капчу\n"
        f"• Редирект на страницу авторизации\n"
        f"• API временно недоступен\n\n"
        f"🛑 <b>Бот остановлен.</b>"
    )


async def notify_failed_brands_and_shutdown(failed_brands: list):
    brands_block = ""
    for entry in failed_brands:
        code  = entry["code"]
        brand = html.escape(entry["brand"])
        url   = html.escape(entry["url"])
        if code == 404:
            icon, reason = "🔴", "404 — ресурс не найден"
        elif code == 403:
            icon, reason = "🟠", "403 — доступ заблокирован (VPN?)"
        else:
            icon, reason = "⚫", f"{code} — неизвестная ошибка"
        brands_block += (
            f"{icon} <b>{brand}</b>\n"
            f"   Ошибка: <code>{reason}</code>\n"
            f"   URL: <code>{url}</code>\n\n"
        )

    codes  = {e["code"] for e in failed_brands}
    causes = ""
    if 404 in codes:
        causes += (
            "📋 <b>Причины 404:</b>\n"
            "• API Vinted изменился\n"
            "• Неверный brand_id\n"
            "• Бренд удалён с платформы\n\n"
        )
    if 403 in codes:
        causes += (
            "📋 <b>Причины 403:</b>\n"
            "• Проблема с VPN/прокси\n"
            "• IP заблокирован Vinted\n"
            "• Требуется смена региона VPN\n\n"
        )

    await notify_all_users(
        f"🚨 <b>КРИТИЧЕСКИЕ ОШИБКИ — {len(failed_brands)} бренд(а/ов)</b>\n\n"
        f"{brands_block}{causes}"
        f"🛑 <b>Бот остановлен.</b>"
    )
    logger.critical(f"🛑 Проблемные бренды: {[e['brand'] for e in failed_brands]}")


# ========== ФУНКЦИИ РАБОТЫ С ДАННЫМИ ==========
def load_cookies() -> dict:
    try:
        with open(COOKIES_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def write_cookies_from_input(session_value: str, access_token_value: str) -> None:
    cookies = load_cookies()
    cookies["__vinted_fr_session"] = session_value
    cookies["access_token_web"]    = access_token_value
    with open(COOKIES_FILE, "w") as f:
        json.dump(cookies, f, ensure_ascii=False)


def load_users() -> set:
    if not os.path.exists(USERS_FILE):
        return set()
    with open(USERS_FILE, "r") as f:
        return {line.strip() for line in f if line.strip()}


def save_user(user_id) -> bool:
    users = load_users()
    if str(user_id) not in users:
        with open(USERS_FILE, "a") as f:
            f.write(f"{user_id}\n")
        logger.info(f"👤 Добавлен пользователь: {user_id}")
        return True
    return False


def remove_user(user_id) -> bool:
    users = load_users()
    u_id  = str(user_id)
    if u_id in users:
        users.remove(u_id)
        with open(USERS_FILE, "w") as f:
            for u in users:
                f.write(f"{u}\n")
        logger.info(f"👤 Удалён пользователь: {user_id}")
        return True
    return False


def load_sent_items(brand_name: str) -> dict:
    filename = STORAGE_FILES.get(brand_name)
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                now = time.time()
                logger.info(f"🔄 [{brand_name}] Конвертирую старый формат → новый")
                return {str(x): now for x in data}
            return {str(k): float(v) for k, v in data.items()}
    except Exception:
        return {}


def save_sent_items(brand_name: str, items_dict: dict):
    filename = STORAGE_FILES.get(brand_name)
    now      = time.time()
    cleaned  = {
        item_id: ts
        for item_id, ts in items_dict.items()
        if (now - ts) < SENT_ITEMS_TTL_SEC
    }
    removed = len(items_dict) - len(cleaned)
    if removed > 0:
        logger.info(
            f"🧹 [{brand_name}] Удалено {removed} устаревших ID "
            f"(>{SENT_ITEMS_TTL_DAYS} дн.) | Осталось: {len(cleaned)}"
        )
    with open(filename, "w") as f:
        json.dump(cleaned, f)


def log_storage_stats():
    total_bytes = 0
    for brand_name, filename in STORAGE_FILES.items():
        try:
            size  = os.path.getsize(filename)
            total_bytes += size
            count = len(load_sent_items(brand_name))
            logger.info(f"📁 {brand_name}: {count} ID | {size / 1024:.1f} КБ")
        except FileNotFoundError:
            logger.info(f"📁 {brand_name}: файл не найден")
    logger.info(f"📦 Итого: {total_bytes / 1024:.1f} КБ")


# ========== ПАРСИНГ TIMESTAMP ==========
def parse_item_ts(raw_ts, boot_time: float) -> Tuple[float, bool]:
    if raw_ts is None:
        return boot_time, False
    try:
        if isinstance(raw_ts, (int, float)):
            ts = float(raw_ts)
        else:
            s = str(raw_ts).strip()
            if not s:
                return boot_time, False
            if s.isdigit():
                ts = float(s)
            else:
                try:
                    if s.endswith("Z"):
                        s = s[:-1] + "+00:00"
                    return datetime.fromisoformat(s).timestamp(), True
                except Exception:
                    return boot_time, False
        if ts > 1e14:
            ts /= 1e6
        elif ts > 1e11:
            ts /= 1e3
        now = time.time()
        if ts < 946684800 or ts > (now + 86400):
            return boot_time, False
        return ts, True
    except Exception:
        return boot_time, False

# Словарь перевода статусов с немецкого на английский
STATUS_TRANSLATIONS = {
    "Gut":                    "Good",
    "Sehr gut":               "Very good",
    "Neu mit Etikett":        "New with tags",
    "Neu ohne Etikett":       "New without tags",
    "Befriedigend":           "Satisfactory",
    "Wie neu":                "Like new",
    # Французские (на случай если попадутся)
    "Bon état":               "Good",
    "Très bon état":          "Very good",
    "Neuf avec étiquette":    "New with tags",
    "Neuf sans étiquette":    "New without tags",
    "État satisfaisant":      "Satisfactory",
}

def build_caption(item: dict) -> str:
    brand      = html.escape(str(item.get("brand", "")))
    title      = html.escape(str(item.get("title", "No title")).replace("_", " "))
    price      = html.escape(str(item.get("price", "")))
    size       = html.escape(str(item.get("size", "N/A")))
    raw_status = str(item.get("status", "N/A"))
    status     = html.escape(STATUS_TRANSLATIONS.get(raw_status, raw_status))
    date     = html.escape(str(item.get("date", "")))
    url      = str(item.get("url", ""))
    url_attr = html.escape(url, quote=True)
    return (
        f"🆕 <b>{brand}</b>\n"
        f"👕 {title}\n"
        f"💰 Цена: {price}\n"
        f"📏 Размер: {size}\n"
        f"⭐ Состояние: {status}\n"
        f"📅 Дата: {date}\n\n"
        f"🔗 <a href=\"{url_attr}\">Открыть на Vinted</a>"
    )


def extract_ts_candidate(obj, now_ts: float) -> Tuple[Any, str]:
    max_depth  = 4
    max_nodes  = 250
    best_value = None
    best_score = None
    best_path  = ""
    nodes      = 0

    def score_ts(ts: float) -> float:
        age = now_ts - ts
        return abs(age) + max(0.0, -age) * 10

    def walk(x, depth: int, path: str):
        nonlocal best_value, best_score, best_path, nodes
        if nodes > max_nodes or depth > max_depth:
            return
        nodes += 1
        if isinstance(x, dict):
            for k, v in x.items():
                key       = str(k)
                next_path = f"{path}.{key}" if path else key
                lowered   = key.lower()
                is_time_key = (
                    lowered.endswith("_ts") or "timestamp" in lowered
                    or "published" in lowered or "created" in lowered
                    or "updated" in lowered or "date" in lowered
                    or "time" in lowered or lowered.endswith("_at")
                    or " at" in lowered or "at_" in lowered
                )
                if is_time_key:
                    try:
                        candidate_ts    = None
                        candidate_value = None
                        if isinstance(v, (int, float)):
                            candidate_ts, candidate_value = float(v), v
                        elif isinstance(v, str):
                            s = v.strip()
                            if s.isdigit():
                                candidate_ts, candidate_value = float(s), v
                            elif s:
                                try:
                                    ss = s[:-1] + "+00:00" if s.endswith("Z") else s
                                    candidate_ts   = datetime.fromisoformat(ss).timestamp()
                                    candidate_value = v
                                except Exception:
                                    pass
                        if candidate_ts is not None:
                            if candidate_ts > 1e14:
                                candidate_ts /= 1e6
                            elif candidate_ts > 1e11:
                                candidate_ts /= 1e3
                            if 946684800 <= candidate_ts <= (now_ts + 86400):
                                s = score_ts(candidate_ts)
                                if best_score is None or s < best_score:
                                    best_score, best_value, best_path = s, candidate_value, next_path
                    except Exception:
                        pass
                walk(v, depth + 1, next_path)
        elif isinstance(x, list):
            for i, v in enumerate(x[:20]):
                walk(v, depth + 1, f"{path}[{i}]")

    walk(obj, 0, "")
    return best_value, best_path


# ========== ПАРСИНГ API VINTED ==========
async def fetch_items(session, brand) -> list:
    cookies     = load_cookies()
    params_list = []
    for k, v in brand["params"].items():
        if isinstance(v, list):
            for val in v:
                params_list.append((k, val))
        else:
            params_list.append((k, v))

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":  "application/json",
        "Referer": "https://www.vinted.de/"
    }
    if cookies.get("access_token_web"):
        headers["Authorization"] = f"Bearer {cookies['access_token_web']}"

    for attempt in range(1, MAX_RETRY_429 + 1):
        try:
            async with session.get(
                brand["url"], params=params_list,
                headers=headers, cookies=cookies, timeout=15
            ) as resp:

                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", BASE_RETRY_DELAY * attempt))
                    logger.warning(f"⚠️ {brand['name']} 429 (попытка {attempt}/{MAX_RETRY_429}, жду {retry_after}с)")
                    if attempt < MAX_RETRY_429:
                        await notify_429(brand["name"], attempt, retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    raise RateLimitError(brand["name"], retry_after)

                content_type = resp.headers.get("Content-Type", "")
                if resp.status == 200 and "application/json" not in content_type:
                    logger.error(f"❌ {brand['name']} неожиданный Content-Type: {content_type}")
                    raise ContentTypeError(brand["name"], brand["url"], content_type)

                if resp.status == 200:
                    data = await resp.json()
                    return data.get("items", []) or []

                if resp.status == 401:
                    logger.error(f"❌ {brand['name']} 401")
                    raise CookiesExpiredError()

                if resp.status == 403:
                    logger.error(f"❌ {brand['name']} 403")
                    raise Vpn403Error(brand["name"], brand["url"])

                if resp.status == 404:
                    logger.error(f"❌ {brand['name']} 404")
                    raise Api404Error(brand["name"], brand["url"])

                logger.error(f"❌ {brand['name']} статус: {resp.status}")
                return []

        except (CookiesExpiredError, Vpn403Error, Api404Error, RateLimitError, ContentTypeError):
            raise
        except Exception as e:
            logger.error(f"💥 Сеть {brand['name']}: {e}")
            return []

    return []


# ========== ОТПРАВКА В TELEGRAM ==========
async def send_notification(item: dict) -> bool:
    caption = build_caption(item)
    users   = load_users()
    if not users:
        return False

    any_success = False
    for user_id in users:
        if item.get("photo"):
            sent = await safe_send_photo(int(user_id), item["photo"], caption)
            if not sent:
                sent = await safe_send_message(int(user_id), caption)
        else:
            sent = await safe_send_message(int(user_id), caption)
        if sent:
            any_success = True
        await asyncio.sleep(0.1)

    return any_success


# ========== ПЛАНИРОВЩИК ==========
async def smart_scheduler():
    global consecutive_401_count

    boot_time          = time.time()
    is_first_cycle     = True
    interval_index     = 0
    debug_failed_ts    = 0
    debug_logged_items = 0

    logger.info("🚀 БОТ ЗАПУЩЕН. Интервалы: 30→60→90 сек. Первый круг фильтрует 15 мин.")
    log_storage_stats()

    async with aiohttp.ClientSession() as session:
        while True:
            await cookies_ready_event.wait()

            total_sent           = 0
            failed_brands        = []
            cookies_need_refresh = False
            should_stop          = False

            for brand in BRANDS:
                brand_name = brand["name"]
                sent_ids   = load_sent_items(brand_name)

                try:
                    items = await fetch_items(session, brand)
                    consecutive_401_count = 0

                except CookiesExpiredError:
                    consecutive_401_count += 1
                    logger.warning(f"⚠️ 401 подряд: {consecutive_401_count}/{MAX_CONSECUTIVE_401}")

                    await notify_all_users(
                        "🔄 <b>Куки протухли.</b> Пробую обновить автоматически через Playwright..."
                    )
                    success = await auto_refresh_cookies()

                    if success:
                        logger.info("✅ Авто-обновление куки прошло успешно")
                        await notify_all_users(
                            "✅ <b>Куки обновлены автоматически!</b>\n"
                            "Продолжаю мониторинг."
                        )
                        consecutive_401_count = 0
                        cookies_need_refresh  = True
                        break
                    else:
                        logger.error("❌ Авто-обновление не помогло")
                        if consecutive_401_count >= MAX_CONSECUTIVE_401:
                            await notify_all_users(
                                "❌ <b>Авто-обновление куки не удалось!</b>\n\n"
                                "Возможные причины:\n"
                                "• Неверный email/password в <code>config.json</code>\n"
                                "• Vinted требует капчу или 2FA\n"
                                "• Сайт изменил форму входа\n\n"
                                "🔐 Пришли вручную значение "
                                "<code>__vinted_fr_session</code> (одной строкой)."
                            )
                            consecutive_401_count = 0
                        await notify_cookies_refresh_needed()
                        cookies_need_refresh = True
                        break

                except RateLimitError as e:
                    await notify_429_fatal(e.brand_name)
                    should_stop = True
                    break

                except ContentTypeError as e:
                    await notify_content_type_error(e.brand_name, e.url, e.content_type)
                    should_stop = True
                    break

                except Api404Error as e:
                    failed_brands.append({"brand": e.brand_name, "url": e.url, "code": 404})
                    continue

                except Vpn403Error as e:
                    failed_brands.append({"brand": e.brand_name, "url": e.url, "code": 403})
                    continue

                logger.info(f"🔎 {brand_name}: найдено {len(items)} поз.")

                for item in items:
                    item_id = str(item.get("id", ""))
                    if not item_id or item_id in sent_ids:
                        continue

                    created_at_ts    = item.get("created_at_ts")
                    created_at       = item.get("created_at")
                    photo_updated_at = item.get("photo_updated_at")
                    raw_ts           = created_at_ts or created_at or photo_updated_at or None

                    ts_candidate, ts_candidate_path = None, ""
                    if raw_ts is None:
                        ts_candidate, ts_candidate_path = extract_ts_candidate(item, boot_time)
                        if ts_candidate is not None:
                            raw_ts = ts_candidate
                    item_ts, ts_ok = parse_item_ts(raw_ts, boot_time)

                    if is_first_cycle and debug_logged_items < 10:
                        age_sec = boot_time - item_ts if ts_ok else None
                        logger.info(
                            f"DEBUG_TS item_id={item_id} ts_ok={ts_ok} "
                            f"created_at_ts={created_at_ts!r} created_at={created_at!r} "
                            f"photo_updated_at={photo_updated_at!r} "
                            f"ts_candidate_path={ts_candidate_path!r} "
                            f"parsed_item_ts={item_ts} age_sec={age_sec}"
                        )
                        if debug_logged_items == 0:
                            at_fields = {
                                k: item.get(k) for k in item.keys()
                                if "at" in k.lower() or k.lower().endswith("_ts")
                            }
                            logger.info(f"DEBUG_TS_FIELDS item_id={item_id} fields={at_fields}")
                            if item.get("photos"):
                                try:
                                    p0 = item["photos"][0] or {}
                                    at_fields_photo = {
                                        k: p0.get(k)
                                        for k in getattr(p0, "keys", lambda: [])()
                                        if "at" in str(k).lower() or str(k).lower().endswith("_ts")
                                    }
                                    logger.info(f"DEBUG_TS_FIELDS_PHOTO item_id={item_id} fields={at_fields_photo}")
                                except Exception:
                                    logger.exception(f"DEBUG_TS_FIELDS_PHOTO failed item_id={item_id}")
                        debug_logged_items += 1

                    if is_first_cycle:
                        ts_source      = (ts_candidate_path or "").lower()
                        ts_is_photo_ts = "photos[" in ts_source or "photo" in ts_source
                        if ts_ok and not ts_is_photo_ts and (boot_time - item_ts) > 900:
                            sent_ids[item_id] = time.time()
                            continue

                    if not ts_ok and debug_failed_ts < 10:
                        logger.warning(f"⚠️ Нет timestamp item_id={item_id} → date=N/A")
                        debug_failed_ts += 1

                    readable_date = (
                        datetime.fromtimestamp(item_ts).strftime('%d.%m.%Y %H:%M')
                        if ts_ok else "N/A"
                    )

                    photo = None
                    if item.get("photos"):
                        p     = item["photos"][0]
                        photo = p.get("full_size_url") or p.get("url")

                    data = {
                        "brand":  brand_name,
                        "title":  item.get("title", "No title").replace("_", " "),
                        "price":  (
                            f"{item.get('price', {}).get('amount', '?')} "
                            f"{item.get('price', {}).get('currency_code', 'EUR')}"
                        ),
                        "size":   item.get("size_title", "N/A"),
                        "status": item.get("status", "N/A"),
                        "date":   readable_date,
                        "url":    f"https://www.vinted.de/items/{item_id}",
                        "photo":  photo
                    }

                    success = await send_notification(data)
                    if success:
                        sent_ids[item_id] = time.time()
                        total_sent += 1
                    else:
                        logger.warning(f"⚠️ Не отправлено item_id={item_id}")
                    await asyncio.sleep(0.4)

                save_sent_items(brand_name, sent_ids)

            # ── Результаты цикла ──────────────────────────────────────────
            if should_stop:
                logger.critical("🛑 Scheduler остановлен.")
                return

            if cookies_need_refresh:
                await cookies_ready_event.wait()
                continue

            if failed_brands:
                await notify_failed_brands_and_shutdown(failed_brands)
                return

            if is_first_cycle:
                logger.info(f"✅ Первый цикл завершён. Отправлено: {total_sent}")
                is_first_cycle = False

            current_wait = STRICT_INTERVALS[interval_index]
            logger.info(f"😴 Пауза {current_wait} сек. (Новых: {total_sent})")
            await asyncio.sleep(current_wait)
            interval_index = (interval_index + 1) % len(STRICT_INTERVALS)


# ========== ОБРАБОТЧИКИ КОМАНД ==========
@dp.message(Command("start"))
async def cmd_start(message: Message):
    save_user(message.chat.id)
    await message.answer(
        "✅ Бот активирован! Вы будете получать уведомления "
        "о вещах, выложенных за последние 15 минут и новее."
    )


@dp.message(Command("stop"))
async def cmd_stop(message: Message):
    if remove_user(message.chat.id):
        await message.answer("📴 Вы отписались от уведомлений.")
    else:
        await message.answer("Вы и так не подписаны.")


@dp.message()
async def cookie_input_handler(message: Message):
    global cookie_refresh_active, cookie_refresh_stage, cookie_temp_session, \
        cookie_temp_access_token, cookie_ask_user_id

    if not cookie_refresh_active:
        return
    if message.text and message.text.strip().startswith("/"):
        return
    if not message.text:
        await message.answer("Пришли текстом значение куки (одной строкой).")
        return

    chat_id = message.chat.id
    if cookie_ask_user_id is None:
        cookie_ask_user_id = chat_id
    elif int(chat_id) != int(cookie_ask_user_id):
        return

    value = message.text.strip()

    if cookie_refresh_stage == 0:
        if not value:
            await message.answer(
                "Значение пустое. Пришли <code>__vinted_fr_session</code> одной строкой.",
                parse_mode="HTML"
            )
            return
        cookie_temp_session  = value
        cookie_refresh_stage = 1
        await message.answer(
            "✅ Принято! Теперь пришли <code>access_token_web</code> (одной строкой).",
            parse_mode="HTML"
        )
        return

    if not value:
        await message.answer(
            "Значение пустое. Пришли <code>access_token_web</code> одной строкой.",
            parse_mode="HTML"
        )
        return

    cookie_temp_access_token = value
    try:
        write_cookies_from_input(cookie_temp_session or "", cookie_temp_access_token or "")
        cookie_refresh_active    = False
        cookie_refresh_stage     = 0
        cookie_temp_session      = None
        cookie_temp_access_token = None
        cookie_ask_user_id       = None
        cookies_ready_event.set()
        await message.answer("✅ Куки обновлены вручную. Возобновляю парсинг.")
    except Exception:
        logger.exception("💥 Не удалось обновить cookies.json")
        await message.answer("❌ Не удалось записать cookies.json. Попробуй ещё раз.")


# ========== ЗАПУСК ==========
async def main():
    logger.info("🤖 Инициализация бота...")

    config = load_config()
    if config.get("vinted_email") and config.get("vinted_password"):
        logger.info("✅ config.json найден — авто-обновление куки включено")
    else:
        logger.warning("⚠️ config.json не найден или неполный — только ручное обновление куки")

    scheduler_task = asyncio.create_task(smart_scheduler())
    polling_task   = asyncio.create_task(dp.start_polling(bot))

    done, pending = await asyncio.wait(
        [scheduler_task, polling_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    if scheduler_task in done:
        logger.info("🛑 Scheduler завершён, останавливаю бота...")
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await bot.session.close()

    logger.critical("🛑 Бот полностью остановлен.")
    exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Программа остановлена.")