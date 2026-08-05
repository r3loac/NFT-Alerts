import asyncio
import json
import logging
import os
import re
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from telethon import TelegramClient, events, utils
from telethon.errors import FloodWaitError
from telethon.network import ConnectionTcpAbridged
from telethon.tl import functions, types


API_ID = int(os.getenv("TG_API_ID", "33834392"))
API_HASH = os.getenv("TG_API_HASH", "8dcb69db8d8e084742f8173a3e826e18")
SESSION = os.getenv("TG_SESSION", "tgsniper")
ALERT_TO = os.getenv("TG_ALERT_TO", "").strip()
ALERT_CHAT_NAME = os.getenv("TG_ALERT_CHAT_NAME", "tg nft").strip()
SETTINGS_FILE = Path(os.getenv("TG_SETTINGS_FILE", "settings.json"))

SCAN_INTERVAL = float(os.getenv("TG_SCAN_INTERVAL", "5"))
SETTINGS_POLL_INTERVAL = max(
    30.0,
    float(os.getenv("TG_SETTINGS_POLL_INTERVAL", "30")),
)
STATUS_UPDATE_INTERVAL = max(
    60.0,
    float(os.getenv("TG_STATUS_UPDATE_INTERVAL", "60")),
)
PER_GIFT_LIMIT = int(os.getenv("TG_PER_GIFT_LIMIT", "100"))
RATE_PER_SEC = float(os.getenv("TG_RATE_PER_SEC", "0.7"))
COMMISSION_REFRESH_SEC = 60 * 60
DEFAULT_EARNINGS_PERMILLE = int(
    os.getenv("TG_RESALE_EARNINGS_PERMILLE", "800")
)
DEFAULT_EXCLUDED_GIFTS = [
    "Plush Pepe",
    "Durov's Cap",
    "Durovs Cap",
    "Durove Cap",
    "Heart Locket",
    "Hear Locket",
]

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("tgsniper")


@dataclass
class Settings:
    backgrounds: list[str] = field(default_factory=list)
    excluded_gifts: list[str] = field(
        default_factory=lambda: list(DEFAULT_EXCLUDED_GIFTS)
    )
    max_price: int = 550
    min_profit: int = 5
    commission_enabled: bool = True
    enabled: bool = True


def _normalize(value: str) -> str:
    return " ".join(value.replace("’", "'").strip().casefold().split())


def _unique_names(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not _normalize(value):
            continue
        key = _normalize(value)
        if key not in seen:
            seen.add(key)
            result.append(value.strip())
    return result


def load_settings() -> Settings:
    if not SETTINGS_FILE.exists():
        return Settings()
    try:
        raw = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        return Settings(
            backgrounds=_unique_names(raw.get("backgrounds", [])),
            excluded_gifts=_unique_names(
                raw.get("excluded_gifts", DEFAULT_EXCLUDED_GIFTS)
            ),
            max_price=max(0, int(raw.get("max_price", 550))),
            min_profit=max(0, int(raw.get("min_profit", 5))),
            commission_enabled=bool(raw.get("commission_enabled", True)),
            enabled=bool(raw.get("enabled", True)),
        )
    except (OSError, ValueError, TypeError) as exc:
        log.error("Не удалось прочитать %s: %s", SETTINGS_FILE, exc)
        return Settings()


def save_settings() -> None:
    temporary = SETTINGS_FILE.with_suffix(SETTINGS_FILE.suffix + ".tmp")
    temporary.write_text(
        json.dumps(asdict(settings), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(SETTINGS_FILE)


settings = load_settings()
earnings_permille = DEFAULT_EARNINGS_PERMILLE
commission_updated_at = 0.0
sent_slugs: set[str] = set()
sent_order: list[str] = []
alert_entity = None
alert_chat_id = 0
account_id = 0
_next_slot = 0.0
_rate_lock = asyncio.Lock()
_message_send_lock = asyncio.Lock()
background_id_cache: dict[tuple[int, tuple[str, ...]], list[int]] = {}
gift_background_ids: dict[int, dict[str, int]] = {}
gift_catalog: dict[int, str] = {}
background_catalog: set[str] = set()
catalog_scanned_gifts: set[int] = set()
catalog_loader_task: asyncio.Task | None = None
processed_setting_ids: set[int] = set()
processed_setting_order: list[int] = []
recent_checks: deque[tuple[str, int | None, str | None]] = deque(maxlen=8)
status_message = None
scan_total = 0
scan_index = 0
scan_current_title = "ожидание запуска"
scan_current_price: int | None = None
scan_current_background: str | None = None
scan_min_price: int | None = None
scan_min_title: str | None = None
scan_alerts = 0
scan_last_title = "ещё ничего"
scan_last_price: int | None = None
scan_last_background: str | None = None

SETTING_PATTERN = re.compile(
    r"^/(?:set|setting)(?:@\w+)?(?:\s+(.*))?$",
    re.IGNORECASE,
)


def _build_proxy():
    raw = os.getenv("TG_PROXY", "").strip()
    if not raw:
        return None
    from urllib.parse import urlparse

    parsed = urlparse(raw)
    if parsed.scheme in ("socks5", "socks4", "http"):
        import socks

        proxy_type = {
            "socks5": socks.SOCKS5,
            "socks4": socks.SOCKS4,
            "http": socks.HTTP,
        }[parsed.scheme]
        return (
            proxy_type,
            parsed.hostname,
            parsed.port,
            True,
            parsed.username,
            parsed.password,
        )
    if parsed.scheme == "mtproxy":
        return ("mtproxy", parsed.hostname, parsed.port, parsed.username)
    raise SystemExit(f"Unknown TG_PROXY scheme: {parsed.scheme}")


client = TelegramClient(
    SESSION,
    API_ID,
    API_HASH,
    connection=ConnectionTcpAbridged,
    proxy=_build_proxy(),
    connection_retries=5,
    retry_delay=2,
    flood_sleep_threshold=0,
)


def _stars_to_int(value) -> int | None:
    """Возвращает цену в Stars, игнорируя варианты цены в TON."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (list, tuple)):
        for item in value:
            result = _stars_to_int(item)
            if result is not None:
                return result
        return None

    type_name = type(value).__name__.casefold()
    if "ton" in type_name or "gram" in type_name:
        return None
    if "star" not in type_name and not hasattr(value, "stars"):
        return None

    amount = getattr(value, "amount", None)
    if amount is not None:
        nanos = getattr(value, "nanos", 0) or 0
        return int(amount) + (1 if nanos > 0 else 0)
    stars = getattr(value, "stars", None)
    return int(stars) if stars is not None else None


def _json_value(value):
    """Преобразует JSONValue из Telegram API в обычные объекты Python."""
    type_name = type(value).__name__
    if type_name == "JsonObject":
        return {item.key: _json_value(item.value) for item in value.value}
    if type_name == "JsonArray":
        return [_json_value(item) for item in value.value]
    if type_name == "JsonNull":
        return None
    if type_name in ("JsonBool", "JsonNumber", "JsonString"):
        return value.value
    return value


async def refresh_commission(force: bool = False) -> None:
    """Получает с сервера долю цены, которую продавец получает после комиссии."""
    global earnings_permille, commission_updated_at
    loop = asyncio.get_running_loop()
    if not force and loop.time() - commission_updated_at < COMMISSION_REFRESH_SEC:
        return
    try:
        response = await client(functions.help.GetAppConfigRequest(hash=0))
        config = _json_value(response.config)
        value = int(config["stars_stargift_resale_commission_permille"])
        if not 0 <= value <= 1000:
            raise ValueError(f"некорректное значение {value}")
        earnings_permille = value
        log.info(
            "Комиссия перепродажи: %.1f%% (продавец получает %.1f%%)",
            commission_percent(),
            earnings_permille / 10,
        )
    except Exception as exc:
        log.warning(
            "Не удалось обновить комиссию Telegram, используется %s/1000: %s",
            earnings_permille,
            exc,
        )
    finally:
        commission_updated_at = loop.time()


def commission_percent() -> float:
    return (1000 - earnings_permille) / 10


def effective_earnings_permille() -> int:
    return earnings_permille if settings.commission_enabled else 1000


def commission_setting_text() -> str:
    if settings.commission_enabled:
        return f"учитывается ({commission_percent():g}%)"
    return f"не учитывается (реальная комиссия Telegram {commission_percent():g}%)"


def sale_proceeds(sale_price: int, permille: int | None = None) -> int:
    multiplier = effective_earnings_permille() if permille is None else permille
    return sale_price * multiplier // 1000


def net_profit(
    purchase_price: int,
    sale_price: int | None,
    permille: int | None = None,
) -> int | None:
    if sale_price is None:
        return None
    return sale_proceeds(sale_price, permille) - purchase_price


def min_profit_for(floor: int | None) -> int:
    return settings.min_profit


def is_excluded(title: str) -> bool:
    title_key = _normalize(title)
    return any(title_key == _normalize(item) for item in settings.excluded_gifts)


def background_is_allowed(background: str | None) -> bool:
    if not settings.backgrounds:
        return True
    if not background:
        return False
    key = _normalize(background)
    return any(key == _normalize(item) for item in settings.backgrounds)


async def list_gifts(include_excluded: bool = False) -> list[tuple[int, str]]:
    response = await client(functions.payments.GetStarGiftsRequest(hash=0))
    result: list[tuple[int, str]] = []
    for gift in getattr(response, "gifts", []):
        gift_id = getattr(gift, "id", None)
        if gift_id is None:
            continue
        has_resale = any(
            getattr(gift, name, None)
            for name in ("resell_min_stars", "resell_min_amount", "availability_resale")
        )
        if not has_resale:
            continue
        title = getattr(gift, "title", None) or str(gift_id)
        gift_catalog[gift_id] = title
        if include_excluded or not is_excluded(title):
            result.append((gift_id, title))
    return result


async def _throttle() -> None:
    global _next_slot
    async with _rate_lock:
        loop = asyncio.get_running_loop()
        now = loop.time()
        wait = _next_slot - now
        if wait > 0:
            await asyncio.sleep(wait)
            now = loop.time()
        _next_slot = max(now, _next_slot) + 1.0 / RATE_PER_SEC


def _lot_from_gift(gift) -> dict[str, Any] | None:
    price = _stars_to_int(getattr(gift, "resell_amount", None))
    if price is None:
        return None
    model = None
    background = None
    for attribute in getattr(gift, "attributes", []) or []:
        class_name = type(attribute).__name__
        if class_name.endswith("Model"):
            model = getattr(attribute, "name", None)
        elif class_name.endswith("Backdrop"):
            background = getattr(attribute, "name", None)
    return {
        "price": price,
        "slug": getattr(gift, "slug", None),
        "model": model,
        "background": background,
    }


async def _request_resale(gift_id: int, attributes=None):
    global _next_slot
    await _throttle()
    try:
        response = await client(
            functions.payments.GetResaleStarGiftsRequest(
                sort_by_price=True,
                sort_by_num=False,
                stars_only=True,
                attributes_hash=0,
                attributes=attributes,
                gift_id=gift_id,
                offset="",
                limit=PER_GIFT_LIMIT,
            )
        )
        catalog_scanned_gifts.add(gift_id)
        per_gift_backgrounds = gift_background_ids.setdefault(gift_id, {})
        for attribute in getattr(response, "attributes", None) or []:
            if type(attribute).__name__.endswith("Backdrop"):
                name = getattr(attribute, "name", None)
                if name:
                    background_catalog.add(name)
                    per_gift_backgrounds[_normalize(name)] = attribute.backdrop_id
        return response
    except FloodWaitError as exc:
        loop = asyncio.get_running_loop()
        _next_slot = max(_next_slot, loop.time() + exc.seconds + 1)
        log.warning("FloodWait %ss — сканирование замедлено", exc.seconds)
        return None
    except Exception as exc:
        log.debug("Ошибка получения лотов %s: %s", gift_id, exc)
        return None


async def cheapest_lot(gift_id: int) -> dict[str, Any] | None:
    background_keys = tuple(sorted(_normalize(item) for item in settings.backgrounds))
    cache_key = (gift_id, background_keys)
    cached_ids = background_id_cache.get(cache_key)
    if background_keys and cached_ids is None and gift_id in gift_background_ids:
        cached_ids = [
            backdrop_id
            for name, backdrop_id in gift_background_ids[gift_id].items()
            if name in background_keys
        ]
        background_id_cache[cache_key] = cached_ids
    if background_keys and cached_ids == []:
        return None

    filters = None
    if cached_ids:
        filters = [types.StarGiftAttributeIdBackdrop(value) for value in cached_ids]
    response = await _request_resale(gift_id, filters)
    if response is None:
        return None

    # The first response also contains the full attribute catalogue. Resolve
    # names to IDs once, then let Telegram filter server-side. This prevents a
    # requested background from being missed when it is outside the cheapest
    # unfiltered page.
    if background_keys and cached_ids is None:
        matched_ids = [
            attribute.backdrop_id
            for attribute in (getattr(response, "attributes", None) or [])
            if type(attribute).__name__.endswith("Backdrop")
            and _normalize(getattr(attribute, "name", "")) in background_keys
        ]
        background_id_cache[cache_key] = matched_ids
        if not matched_ids:
            return None
        filters = [types.StarGiftAttributeIdBackdrop(value) for value in matched_ids]
        response = await _request_resale(gift_id, filters)
        if response is None:
            return None

    lots = []
    for gift in getattr(response, "gifts", None) or []:
        lot = _lot_from_gift(gift)
        if lot is not None and background_is_allowed(lot["background"]):
            lots.append(lot)
    if not lots:
        return None

    lots.sort(key=lambda item: item["price"])
    best = lots[0]
    best["floor"] = lots[1]["price"] if len(lots) > 1 else None
    same_background = [
        lot["price"]
        for lot in lots[1:]
        if lot["background"] == best["background"]
    ]
    best["background_floor"] = same_background[0] if same_background else None
    best["link"] = f"https://t.me/nft/{best['slug']}" if best["slug"] else ""
    return best


def _remember_slug(slug: str) -> None:
    if slug in sent_slugs:
        return
    sent_slugs.add(slug)
    sent_order.append(slug)
    if len(sent_order) > 5000:
        sent_slugs.discard(sent_order.pop(0))


async def send_with_flood_wait(operation, label: str):
    """Сериализует отправки и повторяет их после FloodWait без каскада ошибок."""
    async with _message_send_lock:
        for attempt in range(3):
            try:
                return await operation()
            except FloodWaitError as exc:
                if attempt == 2:
                    raise
                wait = exc.seconds + 2
                log.info("%s: ожидаю лимит Telegram %ss", label, wait)
                await asyncio.sleep(wait)


async def resolve_alert_entity():
    target = ALERT_TO or ALERT_CHAT_NAME
    if not target:
        raise RuntimeError("Не задан TG_ALERT_TO или TG_ALERT_CHAT_NAME")
    if ALERT_TO:
        try:
            target = int(ALERT_TO)
        except ValueError:
            pass
        return await client.get_entity(target)

    wanted = _normalize(ALERT_CHAT_NAME)
    async for dialog in client.iter_dialogs():
        if _normalize(dialog.name or "") == wanted:
            return dialog.entity
    raise RuntimeError(f"Чат для уведомлений не найден: {ALERT_CHAT_NAME!r}")


async def send_lot(title: str, lot: dict[str, Any]) -> bool:
    global alert_entity
    floor = (
        lot["background_floor"] if settings.backgrounds else lot["floor"]
    )
    profit = net_profit(lot["price"], floor)
    if floor is None or profit is None:
        return False
    if lot["price"] > settings.max_price or profit < min_profit_for(floor):
        return False

    proceeds = sale_proceeds(floor)
    if settings.commission_enabled:
        proceeds_line = (
            f"После комиссии {commission_percent():g}%: {proceeds} ⭐"
        )
    else:
        proceeds_line = f"Без учёта комиссии: {proceeds} ⭐"
    text = (
        "🔥 **ВЫГОДНЫЙ ПОДАРОК**\n\n"
        f"🎁 **{title}**\n"
        f"Фон: **{lot['background'] or 'не указан'}**\n"
        f"Модель: {lot['model'] or 'не указана'}\n\n"
        f"Покупка: **{lot['price']} ⭐**\n"
        f"Ориентир продажи: {floor} ⭐\n"
        f"{proceeds_line}\n"
        f"Расчётная прибыль: **+{profit} ⭐**\n\n"
        f"[Открыть подарок]({lot['link']})"
    )
    try:
        await send_with_flood_wait(
            lambda: client.send_message(alert_entity, text, link_preview=False),
            f"Отправка лота {lot['slug']}",
        )
        return True
    except Exception as exc:
        log.error("Не удалось отправить %s: %s", lot["slug"], exc)
        return False


async def send_startup_message(account) -> None:
    global status_message
    text = (
        "🧪 **Тестовое сообщение**\n\n"
        "✅ Снайпер успешно запущен\n"
        f"Аккаунт: **@{account.username or account.id}**\n\n"
        f"{status_text()}"
    )
    try:
        status_message = await send_with_flood_wait(
            lambda: client.send_message(alert_entity, text, link_preview=False),
            "Тестовое сообщение",
        )
        log.info("Тестовое сообщение отправлено в %s", ALERT_TO or ALERT_CHAT_NAME)
    except Exception:
        log.exception("Не удалось отправить тестовое сообщение при запуске")


def status_text() -> str:
    if scan_last_price is None:
        last_result = f"{scan_last_title}: нет подходящих лотов"
    else:
        last_result = f"{scan_last_title}: {scan_last_price} ⭐"
    if scan_min_price is None:
        minimum = "ещё не найдена"
    else:
        minimum = f"{scan_min_price} ⭐ — {scan_min_title}"
    backgrounds = ", ".join(settings.backgrounds) or "все"
    recent = []
    for title, price, background in reversed(recent_checks):
        price_text = f"{price} ⭐" if price is not None else "нет подходящих лотов"
        background_text = f" · {background}" if background else ""
        recent.append(f"• {title}: {price_text}{background_text}")
    recent_text = "\n".join(recent) or "• список пока пуст"
    return (
        "📊 **Живой статус снайпера**\n\n"
        f"Сейчас проверяется: **{scan_current_title}**\n"
        f"Прогресс: **{scan_index}/{scan_total}**\n"
        f"Последний результат: **{last_result}**\n"
        f"Фон последнего лота: **{scan_last_background or '—'}**\n"
        f"Самая низкая цена в этом цикле: **{minimum}**\n"
        f"Выгодных лотов в этом цикле: **{scan_alerts}**\n"
        f"Фильтр фонов: **{backgrounds}**\n"
        f"Комиссия в расчёте: **{commission_setting_text()}**\n\n"
        f"**Последние проверки**\n{recent_text}"
    )


async def update_status_message() -> None:
    global status_message
    last_text = None
    next_delay = STATUS_UPDATE_INTERVAL
    while True:
        if next_delay > 0:
            await asyncio.sleep(next_delay)
        next_delay = STATUS_UPDATE_INTERVAL
        try:
            text = status_text()
            if text != last_text:
                if status_message is None:
                    status_message = await client.send_message(
                        alert_entity,
                        text,
                        link_preview=False,
                    )
                else:
                    await status_message.edit(text, link_preview=False)
                last_text = text
        except asyncio.CancelledError:
            raise
        except FloodWaitError as exc:
            next_delay = max(STATUS_UPDATE_INTERVAL, exc.seconds + 5)
            log.info(
                "Следующее обновление статуса через %ss из-за лимита Telegram",
                int(next_delay),
            )
        except Exception:
            log.exception("Не удалось обновить живой статус")
            status_message = None
            next_delay = STATUS_UPDATE_INTERVAL


async def scan_once() -> None:
    global scan_total, scan_index, scan_current_title
    global scan_current_price, scan_current_background
    global scan_min_price, scan_min_title
    global scan_last_title, scan_last_price, scan_last_background
    global scan_alerts
    await refresh_commission()
    gifts = await list_gifts()
    scan_total = len(gifts)
    scan_index = 0
    scan_min_price = None
    scan_min_title = None
    scan_alerts = 0
    log.info("Сканирование %s типов подарков", len(gifts))
    for index, (gift_id, title) in enumerate(gifts, start=1):
        if not settings.enabled:
            return
        scan_index = index
        scan_current_title = title
        scan_current_price = None
        scan_current_background = None
        lot = await cheapest_lot(gift_id)
        if lot is None:
            scan_last_title = title
            scan_last_price = None
            scan_last_background = None
            recent_checks.append((title, None, None))
            continue
        scan_current_price = lot["price"]
        scan_current_background = lot["background"]
        scan_last_title = title
        scan_last_price = lot["price"]
        scan_last_background = lot["background"]
        recent_checks.append((title, lot["price"], lot["background"]))
        if scan_min_price is None or lot["price"] < scan_min_price:
            scan_min_price = lot["price"]
            scan_min_title = title
        if not lot["slug"] or lot["slug"] in sent_slugs:
            continue
        if await send_lot(title, lot):
            _remember_slug(lot["slug"])
            scan_alerts += 1
            log.info("Отправлен %s за %s Stars", lot["slug"], lot["price"])
    scan_current_title = "цикл завершён, ожидаю следующий"


async def monitor() -> None:
    while True:
        try:
            if settings.enabled:
                await scan_once()
        except FloodWaitError as exc:
            log.warning("FloodWait %ss", exc.seconds)
            await asyncio.sleep(exc.seconds + 1)
        except Exception:
            log.exception("Ошибка цикла мониторинга")
        await asyncio.sleep(SCAN_INTERVAL)


async def load_catalogs() -> None:
    """Фоново собирает все названия подарков и фонов из Telegram."""
    try:
        gifts = await list_gifts(include_excluded=True)
        for gift_id, _ in reversed(gifts):
            if gift_id in catalog_scanned_gifts:
                continue
            await _request_resale(gift_id)
        log.info(
            "Каталоги загружены: %s подарков, %s фонов",
            len(gift_catalog),
            len(background_catalog),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Не удалось полностью загрузить каталоги настроек")


def ensure_catalog_loader() -> None:
    global catalog_loader_task
    if gift_catalog and set(gift_catalog).issubset(catalog_scanned_gifts):
        return
    if catalog_loader_task is None or catalog_loader_task.done():
        catalog_loader_task = asyncio.create_task(load_catalogs())


def _catalog_progress() -> str:
    total = len(gift_catalog)
    done = min(len(catalog_scanned_gifts), total)
    if total and done < total:
        return f"Каталог фонов загружается: {done}/{total} типов"
    return f"Распознано фонов: {len(background_catalog)}"


def settings_text(note: str | None = None) -> str:
    backgrounds = ", ".join(settings.backgrounds) or "все"
    excluded = ", ".join(settings.excluded_gifts) or "нет"
    available_backgrounds = ", ".join(sorted(background_catalog, key=_normalize))
    if not available_backgrounds:
        available_backgrounds = "загружаются автоматически…"
    status = "включён" if settings.enabled else "выключен"
    prefix = f"✅ {note}\n\n" if note else ""
    return (
        f"{prefix}⚙️ **Настройки снайпера**\n\n"
        f"Статус: **{status}**\n"
        f"Фоны: **{backgrounds}**\n"
        f"Исключены подарки: **{excluded}**\n"
        f"Максимальная цена: **{settings.max_price} ⭐**\n"
        f"Минимальная прибыль: **{settings.min_profit} ⭐**\n"
        f"Комиссия в расчёте: **{commission_setting_text()}**\n"
        f"{_catalog_progress()}\n"
        f"Доступные фоны: {available_backgrounds}\n\n"
        "**Команды**\n"
        "`/set background add <название>`\n"
        "`/set background del <название>`\n"
        "`/set background clear` — разрешить все фоны\n"
        "`/set exclude add <название подарка>`\n"
        "`/set exclude del <название подарка>`\n"
        "`/set exclude clear`\n"
        "`/set maxprice <Stars>`\n"
        "`/set minprofit <Stars>`\n"
        "`/set on` или `/set off`\n"
        "`/set commission on|off` — включить или выключить учёт комиссии\n"
        "`/set commission` — обновить комиссию Telegram"
    )


def _change_name_list(items: list[str], action: str, name: str) -> str:
    key = _normalize(name)
    existing = next((item for item in items if _normalize(item) == key), None)
    if action == "add":
        if existing:
            return f"{existing!r} уже есть в списке"
        items.append(name.strip())
        save_settings()
        return f"Добавлено: {name.strip()}"
    if action == "del":
        if not existing:
            return f"{name.strip()!r} не найдено в списке"
        items.remove(existing)
        save_settings()
        return f"Удалено: {existing}"
    raise ValueError("Неизвестное действие. Используйте add, del или clear")


async def apply_setting_command(argument: str) -> str:
    parts = argument.strip().split()
    if not parts:
        return settings_text()

    command = parts[0].casefold()
    if command in ("on", "вкл", "включить"):
        settings.enabled = True
        save_settings()
        return settings_text("Мониторинг включён")
    if command in ("off", "выкл", "выключить"):
        settings.enabled = False
        save_settings()
        return settings_text("Мониторинг выключен")
    if command in ("commission", "комиссия"):
        if len(parts) == 1:
            await refresh_commission(force=True)
            return settings_text("Комиссия Telegram обновлена")
        value = parts[1].casefold()
        if value in ("on", "вкл", "включить"):
            settings.commission_enabled = True
            save_settings()
            return settings_text("Учёт комиссии включён")
        if value in ("off", "выкл", "выключить"):
            settings.commission_enabled = False
            save_settings()
            return settings_text("Учёт комиссии выключен")
        raise ValueError("Формат: /set commission on|off")
    if command in ("maxprice", "цена"):
        if len(parts) != 2 or not parts[1].isdigit():
            raise ValueError("Формат: /set maxprice <Stars>")
        settings.max_price = int(parts[1])
        save_settings()
        return settings_text("Максимальная цена изменена")
    if command in ("minprofit", "прибыль"):
        if len(parts) != 2 or not parts[1].isdigit():
            raise ValueError("Формат: /set minprofit <Stars>")
        settings.min_profit = int(parts[1])
        save_settings()
        return settings_text("Минимальная прибыль изменена")

    list_aliases = {
        "background": settings.backgrounds,
        "backgrounds": settings.backgrounds,
        "bg": settings.backgrounds,
        "фон": settings.backgrounds,
        "фоны": settings.backgrounds,
        "exclude": settings.excluded_gifts,
        "excluded": settings.excluded_gifts,
        "исключить": settings.excluded_gifts,
        "подарок": settings.excluded_gifts,
    }
    items = list_aliases.get(command)
    if items is None:
        raise ValueError("Неизвестная настройка. Отправьте /set для справки")
    if len(parts) < 2:
        raise ValueError("Укажите add, del или clear")

    action_aliases = {
        "add": "add",
        "+": "add",
        "добавить": "add",
        "del": "del",
        "delete": "del",
        "remove": "del",
        "-": "del",
        "удалить": "del",
        "clear": "clear",
        "очистить": "clear",
    }
    action = action_aliases.get(parts[1].casefold())
    if action == "clear":
        items.clear()
        save_settings()
        label = "Фильтр фонов очищен" if items is settings.backgrounds else "Исключения очищены"
        return settings_text(label)
    if action not in ("add", "del") or len(parts) < 3:
        raise ValueError("Формат: /set background|exclude add|del <название>")
    note = _change_name_list(items, action, " ".join(parts[2:]))
    return settings_text(note)


def _remember_setting_message(message_id: int) -> bool:
    if message_id in processed_setting_ids:
        return False
    processed_setting_ids.add(message_id)
    processed_setting_order.append(message_id)
    if len(processed_setting_order) > 500:
        processed_setting_ids.discard(processed_setting_order.pop(0))
    return True


async def handle_setting_message(message, argument: str) -> None:
    if not _remember_setting_message(message.id):
        return
    try:
        await client(
            functions.messages.SendReactionRequest(
                peer=message.chat_id,
                msg_id=message.id,
                reaction=[types.ReactionEmoji(emoticon="👀")],
            )
        )
    except Exception:
        log.debug("Не удалось поставить реакцию на /set", exc_info=True)
    try:
        ensure_catalog_loader()
        text = await apply_setting_command(argument)
    except (ValueError, OSError) as exc:
        text = f"❌ {exc}\n\n{settings_text()}"
    except Exception as exc:
        log.exception("Ошибка обработки /set")
        text = f"❌ Ошибка обработки /set: `{exc}`"
    try:
        await send_with_flood_wait(
            lambda: message.reply(text, link_preview=False),
            "Ответ на /set",
        )
    except Exception:
        log.exception("Не удалось отправить итоговый ответ на /set")


@client.on(events.NewMessage(pattern=SETTING_PATTERN))
async def setting_handler(event) -> None:
    if not alert_chat_id or event.chat_id != alert_chat_id:
        return
    await handle_setting_message(event, event.pattern_match.group(1) or "")


async def poll_setting_messages() -> None:
    """Редкий резервный опрос команд, если клиент не прислал NewMessage update."""
    latest = await client.get_messages(alert_entity, limit=1)
    last_message_id = latest[0].id if latest else 0
    while True:
        await asyncio.sleep(SETTINGS_POLL_INTERVAL)
        try:
            messages = await client.get_messages(alert_entity, limit=20)
            new_messages = [message for message in messages if message.id > last_message_id]
            for message in reversed(new_messages):
                last_message_id = max(last_message_id, message.id)
                match = SETTING_PATTERN.match(message.raw_text or "")
                if match:
                    await handle_setting_message(message, match.group(1) or "")
        except asyncio.CancelledError:
            raise
        except FloodWaitError as exc:
            log.info("Проверка /set продолжится через %ss", exc.seconds + 5)
            await asyncio.sleep(exc.seconds + 5)
        except Exception:
            log.exception("Ошибка резервной проверки /set")


async def main() -> None:
    global alert_entity, alert_chat_id, account_id
    await client.start()
    me = await client.get_me()
    account_id = me.id
    alert_entity = await resolve_alert_entity()
    alert_chat_id = utils.get_peer_id(alert_entity)
    await refresh_commission(force=True)
    await send_startup_message(me)
    log.info(
        "Снайпер запущен: @%s, чат уведомлений: %s",
        me.username or me.id,
        ALERT_TO or ALERT_CHAT_NAME,
    )
    monitor_task = asyncio.create_task(monitor())
    status_task = asyncio.create_task(update_status_message())
    settings_task = asyncio.create_task(poll_setting_messages())
    try:
        await client.run_until_disconnected()
    finally:
        monitor_task.cancel()
        status_task.cancel()
        settings_task.cancel()
        tasks = [monitor_task, status_task, settings_task]
        if catalog_loader_task is not None:
            catalog_loader_task.cancel()
            tasks.append(catalog_loader_task)
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Снайпер остановлен пользователем")
