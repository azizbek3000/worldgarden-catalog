#!/usr/bin/env python3
"""
Собирает готовую базу каталога app/src/main/assets/databases/catalog.db.

Приложение не импортирует каталог из JSON: Room копирует этот файл
на устройство как есть (см. data/db/CatalogDatabase.java).

Что попадает в базу:
  - cities          — из app/src/main/assets/data/cities.json (GeoNames,
                      собирается tools/build_cities.py);
  - tv_channels     — из iptv-org (https://github.com/iptv-org/api):
                      только каналы, у которых есть хотя бы один рабочий
                      поток, не закрытые и не "для взрослых";
  - streams         — потоки этих каналов (до --max-streams на канал).
                      Если есть tools/cache/stream_status.json (его пишет
                      tools/check_streams.py), нерабочие потоки не берутся;
  - radio_stations  — из Radio Browser (https://www.radio-browser.info):
                      до --radio-per-country самых популярных рабочих
                      станций каждой страны, прошедших проверку страны
                      (см. CountryCheck); у каждой один поток в streams.
                      Плюс ручные дополнения из tools/data/radio_extra.json.

Привязка канала к городу: если у канала в iptv-org указан город вещания
и он нашёлся в GeoNames по названию и стране — этот город, иначе столица
страны канала. Станция привязывается к ближайшему городу по своим
координатам (не дальше 60 км), иначе по названию региона, иначе к столице.

Таблицы создаются по схеме Room из app/schemas (её выгружает сборка
проекта), поэтому после изменения сущностей каталога нужно:

    gradlew.bat assembleDebug
    py tools/build_catalog.py

Запуск из корня проекта:

    py tools/build_catalog.py            # данные из tools/cache
    py tools/build_catalog.py --refresh  # скачать данные заново

Параметры:

    --refresh        скачать данные iptv-org и Radio Browser заново (иначе
                     берутся из tools/cache, скачиваются только недостающие)
    --max-streams N  сколько потоков хранить на канал (по умолчанию 5)
    --radio-per-country N
                     сколько станций брать на страну (по умолчанию 50)
"""

import argparse
import collections
import concurrent.futures
import datetime
import gzip
import json
import math
import os
import re
import sqlite3
import sys
import unicodedata
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

API = "https://iptv-org.github.io/api/"

# Решение пользователя (2026-10-08): все каналы и станции привязаны только
# к столицам стран — иначе на глобусе слишком много городов с контентом
# (пригороды со своими станциями вокруг каждого крупного города).
# False вернёт привязку к городу из источника.
CAPITALS_ONLY = True

# Взрослый контент (2026-10-15): за него приложение удалят из Play Market.
# Флаг is_nsfw и категория xxx в iptv-org стоят не у всех таких каналов,
# поэтому дополнительно отсеиваем по названию (и по тегам у радио).
# Слова — целиком: RMF MAXXX, XXXLutz, Sextavision, Adulto Joven не задеты.
# Греческое «Ερωτικός» — «о любви», это обычные станции любовных песен.
ADULT_NAME_RE = re.compile(
    r"\b(sex|sexy|xxx|porn\w*|erotic\w*|erotik\w*|adult|adults|nsfw|hentai|playboy|"
    r"hustler|penthouse|brazzers|dorcel|redlight|fetish|bdsm|milf|nude|naked)\b"
    r"|(?<![a-z])sexy?(?![a-z])|(?<![a-z])sex(?=fm|tv|radio)"
    r"|18\s*\+|\+\s*18|эрот\w*|секс\w*|порн\w*|для взрослых",
    re.I,
)
# Теги радио: «adult contemporary» — музыкальный формат, его не трогаем.
ADULT_TAG_RE = re.compile(
    r"\b(sex|sexy|xxx|porn\w*|erotic\w*|erotik\w*|nsfw|hentai|fetish)\b|18\s*\+",
    re.I,
)
# Каналы, которые не выдают себя названием: фильмы с откровенными сценами.
ADULT_CHANNEL_IDS = {
    "MidnightPulp.us",
}


def is_adult_name(*names):
    return any(name and ADULT_NAME_RE.search(name) for name in names)

CACHE_DIR = os.path.join(ROOT, "tools", "cache", "iptv")
CITIES_JSON = os.path.join(ROOT, "app", "src", "main", "assets", "data", "cities.json")
STATUS_JSON = os.path.join(ROOT, "tools", "cache", "stream_status.json")
SCHEMA_JSON = os.path.join(
    ROOT, "app", "schemas", "com.asis.worldgarden.data.db.CatalogDatabase", "1.json"
)
OUTPUT_DB = os.path.join(ROOT, "app", "src", "main", "assets", "databases", "catalog.db")
# Версия каталога рядом с базой: по ней приложение решает, новее ли скачанный
# каталог, чем вшитый (data/CatalogUpdater, data/db/CatalogDatabase).
OUTPUT_INFO = os.path.join(ROOT, "app", "src", "main", "assets", "databases", "catalog.json")

RADIO_CACHE_DIR = os.path.join(ROOT, "tools", "cache", "radio")

# Станции, добавленные вручную: id, name, countryCode, url, source и по
# желанию cityId, language, website, genre. Без cityId — столица страны.
RADIO_EXTRA_JSON = os.path.join(ROOT, "tools", "data", "radio_extra.json")

# Языки стран (из данных локалей Windows) — для проверки страны станции.
COUNTRY_LANGUAGES_JSON = os.path.join(ROOT, "tools", "data", "country_languages.json")

# Серверы Radio Browser: пробуются по очереди.
RADIO_API_HOSTS = (
    "de1.api.radio-browser.info",
    "de2.api.radio-browser.info",
    "fi1.api.radio-browser.info",
    "nl1.api.radio-browser.info",
)

# Страны, где английский — основной язык вещания (см. CountryCheck).
ENGLISH_COUNTRIES = frozenset((
    "US GB CA AU NZ IE ZA NG GH KE UG TZ ZM ZW MW BW NA LR SL GM JM TT BS BB BZ GY "
    "AG DM GD KN LC VC SG PH IN PK MT FJ PG SB VU WS TO KI TV NR PW FM MH GU VI BM "
    "KY VG AI MS TC GI IM JE GG FK SH"
).split())

# Национальные домены, которые берут ради красивого адреса (radio.fm, live.tv),
# а не из-за страны: чужой страной такой домен не считается.
VANITY_DOMAINS = frozenset((
    "FM TV CO ME IO AM LY TO CC WS NU TK ML GA CF GQ AI GG IM DJ MU LA AC SH ST VC "
    "BZ CX MS PW TC VG SC IS AS BE"
).split())

# Координаты станции должны быть не дальше этого от какого-нибудь города страны.
RADIO_COUNTRY_MAX_KM = 150.0

# Станция привязывается к ближайшему городу, если он не дальше этого расстояния.
RADIO_CITY_MAX_KM = 60.0

# Сколько станций страны запрашивать: сначала немного, а если после проверки
# страны не набралось нужного числа — один раз список длиннее.
RADIO_BASE_LIMIT = 70
RADIO_MORE_LIMIT = 250
# Третья попытка — для больших стран, где и 250 кандидатов не хватило.
RADIO_MOST_LIMIT = 700

# iptv-org пишет Великобританию как "UK", GeoNames и Android — как "GB".
COUNTRY_FIX = {"UK": "GB"}

# Категории iptv-org -> TvCategory (model/TvCategory.java). У канала берётся
# первая его категория, которая здесь есть.
CATEGORY_MAP = {
    "news": "NEWS",
    "sports": "SPORT",
    "music": "MUSIC",
    "movies": "MOVIES",
    "series": "MOVIES",
    "classic": "MOVIES",
    "kids": "KIDS",
    "animation": "KIDS",
    "documentary": "DOCUMENTARY",
    "science": "DOCUMENTARY",
    "travel": "DOCUMENTARY",
    "outdoor": "DOCUMENTARY",
    "culture": "DOCUMENTARY",
    "education": "EDUCATION",
    "entertainment": "ENTERTAINMENT",
    "comedy": "ENTERTAINMENT",
    "family": "ENTERTAINMENT",
    "interactive": "ENTERTAINMENT",
    "lifestyle": "LIFESTYLE",
    "cooking": "LIFESTYLE",
    "relax": "LIFESTYLE",
    "auto": "LIFESTYLE",
    "business": "BUSINESS",
    "religious": "RELIGIOUS",
    "shop": "SHOPPING",
    "weather": "WEATHER",
    "legislative": "GOVERNMENT",
    "public": "GOVERNMENT",
    "general": "GENERAL",
}

# Теги Radio Browser -> RadioGenre (model/RadioGenre.java). Порядок важен:
# берётся первый жанр, одно из слов которого встретилось в тегах станции.
GENRE_RULES = (
    ("NEWS", ("news", "noticias", "nachrichten", "actualit", "новост", "information")),
    ("SPORT", ("sport", "football", "futbol", "fútbol", "deportes", "soccer")),
    ("KIDS", ("kids", "children", "child", "infantil", "kinder", "детск")),
    ("RELIGIOUS", ("christian", "gospel", "relig", "catholic", "católic", "islam", "quran",
                   "bible", "worship", "church", "cristian", "evangel", "spiritual",
                   "devotional", "mantra", "hindu", "православ")),
    ("CLASSICAL", ("classical", "klassik", "opera", "symphon", "baroque", "классик")),
    ("METAL", ("metal", "hardcore")),
    ("ALTERNATIVE", ("indie", "alternative", "punk", "grunge")),
    ("ROCK", ("rock", "рок")),
    ("BLUES", ("blues", "soul", "funk", "motown")),
    ("JAZZ", ("jazz", "swing", "bossa", "джаз")),
    ("REGGAE", ("reggae", "ska", "dancehall")),
    ("LATIN", ("latin", "salsa", "cumbia", "bachata", "merengue", "tropical", "reggaeton",
               "sertanejo", "mpb", "ranchera", "tango", "baladas")),
    ("HIPHOP", ("hip hop", "hip-hop", "hiphop", "rap", "r&b", "rnb", "urban", "trap")),
    ("CHILLOUT", ("ambient", "chillout", "chill", "lounge", "relax", "easy listening",
                  "new age", "meditation", "smooth")),
    ("DANCE", ("dance", "disco", "party", "club")),
    ("ELECTRONIC", ("electro", "techno", "house", "trance", "edm",
                    "drum and bass", "dubstep")),
    ("OLDIES", ("oldies", "retro", "ретро", "60s", "70s", "80s", "90s", "80's", "90's",
                "2000s", "classic hits", "schlager", "flashback", "nostalgi", "golden")),
    ("COUNTRY", ("country", "bluegrass", "americana")),
    ("FOLK", ("folk", "world music", "traditional", "ethnic", "regional", "bollywood",
              "narodna", "manele", "folclore", "шансон")),
    ("TALK", ("talk", "speech", "spoken", "comedy", "podcast", "storytelling")),
    ("CULTURE", ("culture", "cultura", "kultur", "education", "university", "college",
                 "public radio", "community")),
    ("POP", ("pop", "top 40", "top40", "hits", "charts", "adult contemporary",
             "love songs", "ballad", "romantic")),
)

# ISO 639-3 (iptv-org) -> ISO 639-1 (его понимает Locale в Android).
# Языков, которых здесь нет, остаётся трёхбуквенный код.
LANGUAGE_MAP = {
    "eng": "en", "spa": "es", "fra": "fr", "deu": "de", "ita": "it", "por": "pt",
    "rus": "ru", "ara": "ar", "zho": "zh", "cmn": "zh", "yue": "zh", "jpn": "ja",
    "kor": "ko", "hin": "hi", "ben": "bn", "urd": "ur", "tur": "tr", "fas": "fa",
    "pes": "fa", "nld": "nl", "pol": "pl", "ukr": "uk", "ron": "ro", "ell": "el",
    "ces": "cs", "slk": "sk", "hun": "hu", "bul": "bg", "srp": "sr", "hrv": "hr",
    "bos": "bs", "slv": "sl", "mkd": "mk", "sqi": "sq", "swe": "sv", "nor": "no",
    "nob": "nb", "dan": "da", "fin": "fi", "isl": "is", "est": "et", "lav": "lv",
    "lit": "lt", "heb": "he", "tha": "th", "vie": "vi", "ind": "id", "msa": "ms",
    "zsm": "ms", "fil": "fil", "tgl": "tl", "tam": "ta", "tel": "te", "mal": "ml",
    "kan": "kn", "mar": "mr", "guj": "gu", "pan": "pa", "nep": "ne", "sin": "si",
    "mya": "my", "khm": "km", "lao": "lo", "mon": "mn", "kaz": "kk", "uzb": "uz",
    "kir": "ky", "tgk": "tg", "tuk": "tk", "aze": "az", "hye": "hy", "kat": "ka",
    "bel": "be", "cat": "ca", "eus": "eu", "glg": "gl", "gle": "ga", "cym": "cy",
    "mlt": "mt", "ltz": "lb", "afr": "af", "swa": "sw", "swh": "sw", "amh": "am",
    "som": "so", "hau": "ha", "yor": "yo", "ibo": "ig", "zul": "zu", "xho": "xh",
    "kur": "ku", "ckb": "ckb", "pus": "ps", "prs": "fa", "snd": "sd", "asm": "as",
    "ori": "or", "bod": "bo", "uig": "ug", "kin": "rw", "mlg": "mg", "hat": "ht",
    "que": "qu", "grn": "gn", "aym": "ay", "tat": "tt", "epo": "eo", "lat": "la",
}


def log(*args):
    print(*args, flush=True)


# ---------------------------------------------------------------- iptv-org

def fetch(name, refresh):
    """Возвращает JSON iptv-org; скачивает в кэш, если файла нет."""

    path = os.path.join(CACHE_DIR, name + ".json")

    if refresh or not os.path.exists(path):
        os.makedirs(CACHE_DIR, exist_ok=True)
        url = API + name + ".json"
        log("downloading", url)
        request = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
        with urllib.request.urlopen(request, timeout=600) as response:
            data = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                data = gzip.decompress(data)
        with open(path, "wb") as f:
            f.write(data)

    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ------------------------------------------------------------------ города

def normalize(name):
    """ "São Paulo" -> "saopaulo": без диакритики, регистра и знаков. """

    text = unicodedata.normalize("NFKD", name)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return "".join(c for c in text.lower() if c.isalnum())


class CityIndex:
    """Поиск города GeoNames по стране и названию, столицы по стране."""

    def __init__(self, cities):
        self.by_name = {}
        self.capital = {}
        self.by_country = collections.defaultdict(list)

        # cities.json отсортирован по населению: первый с таким именем — крупнейший.
        for city in cities:
            country = city["countryCode"]
            self.by_name.setdefault((country, normalize(city["name"])), city["id"])
            self.by_country[country].append(
                (city["latitude"], city["longitude"], city["id"])
            )
            if city.get("capital"):
                self.capital.setdefault(country, city["id"])

    def find(self, country, name):
        key = normalize(name)
        for candidate in (key, key + "city", key.removesuffix("city")):
            city_id = self.by_name.get((country, candidate))
            if city_id is not None:
                return city_id
        return None

    def nearest(self, country, lat, lon, max_km):
        """Ближайший город страны не дальше max_km, иначе None."""

        best_id, best_km = None, max_km

        for city_lat, city_lon, city_id in self.by_country.get(country, ()):
            # Быстрая отсечка по широте: 1 градус — около 111 км.
            if abs(city_lat - lat) * 111.0 > best_km:
                continue
            p1, p2 = math.radians(lat), math.radians(city_lat)
            a = (math.sin((p2 - p1) / 2) ** 2
                 + math.cos(p1) * math.cos(p2)
                 * math.sin(math.radians(city_lon - lon) / 2) ** 2)
            km = 12742.0 * math.asin(math.sqrt(a))
            if km < best_km:
                best_id, best_km = city_id, km

        return best_id


# ---------------------------------------------------------------------- ТВ

def pick_logos(logos):
    """channel id -> url логотипа (один на канал, лучший из logos.json iptv-org)."""

    def rank(logo):
        return (
            # Логотип самого канала, а не его регионального фида.
            logo.get("feed") is not None,
            # Ширины 200+ хватает для плитки; из подходящих берём самый лёгкий.
            (logo.get("width") or 0) < 200,
            abs((logo.get("width") or 0) - 512),
        )

    best = {}
    for logo in logos:
        url = logo.get("url") or ""
        if not (logo.get("channel") and url.startswith(("http://", "https://"))):
            continue
        # Glide в приложении не показывает SVG, а AVIF есть не на всех Android.
        if (logo.get("format") or "").lower() in ("svg", "avif"):
            continue
        current = best.get(logo["channel"])
        if current is None or rank(logo) < rank(current):
            best[logo["channel"]] = logo

    return {channel: logo["url"] for channel, logo in best.items()}


def build_tv(index, max_streams, per_country, refresh):

    channels = fetch("channels", refresh)
    streams = fetch("streams", refresh)
    feeds = fetch("feeds", refresh)
    iptv_cities = {c["code"]: c for c in fetch("cities", refresh)}
    logos = pick_logos(fetch("logos", refresh))

    feeds_by_channel = collections.defaultdict(list)
    for feed in feeds:
        feeds_by_channel[feed["channel"]].append(feed)

    # Результаты tools/check_streams.py: {url: {"ok": bool, "reason": str}}.
    # Непроверенные потоки считаются рабочими.
    status = {}
    if os.path.exists(STATUS_JSON):
        with open(STATUS_JSON, encoding="utf-8") as f:
            status = json.load(f)

    stats = collections.Counter()

    streams_by_channel = collections.defaultdict(list)
    for stream in streams:
        if not (stream.get("channel") and stream.get("url")):
            continue
        # rtmp, rtsp, srt, mmsh плеер приложения не играет.
        if not stream["url"].startswith(("http://", "https://")):
            stats["streams dropped: scheme"] += 1
            continue
        if not status.get(stream["url"], {"ok": True})["ok"]:
            stats["streams dropped: dead"] += 1
            continue
        streams_by_channel[stream["channel"]].append(stream)

    tv_rows = []
    stream_rows = []

    for channel in channels:

        channel_streams = streams_by_channel.get(channel["id"])

        if not channel_streams:
            stats["skipped: no streams"] += 1
            continue
        if channel.get("closed"):
            stats["skipped: closed"] += 1
            continue
        if channel.get("is_nsfw") or "xxx" in (channel.get("categories") or []):
            stats["skipped: nsfw"] += 1
            continue
        if (channel["id"] in ADULT_CHANNEL_IDS
                or is_adult_name(channel.get("name"), *(channel.get("alt_names") or []))):
            stats["skipped: adult name"] += 1
            continue

        country = COUNTRY_FIX.get(channel["country"], channel["country"])

        channel_feeds = feeds_by_channel.get(channel["id"], [])
        # Основной фид первым: по нему определяем город и язык.
        channel_feeds.sort(key=lambda f: not f.get("is_main"))
        main_feed = channel_feeds[0]["id"] if channel_feeds else None

        city_id = None
        # CAPITALS_ONLY: город канала не ищем, все каналы — в столице страны.
        for feed in ([] if CAPITALS_ONLY else channel_feeds):
            for area in feed.get("broadcast_area") or []:
                if not area.startswith("ct/"):
                    continue
                iptv_city = iptv_cities.get(area[3:])
                if iptv_city is None:
                    continue
                city_country = COUNTRY_FIX.get(iptv_city["country"], iptv_city["country"])
                city_id = index.find(city_country, iptv_city["name"])
                if city_id is not None:
                    break
            if city_id is not None:
                break

        national = city_id is None

        if city_id is not None:
            stats["city: own"] += 1
        else:
            city_id = index.capital.get(country)
            if city_id is None:
                stats["skipped: no capital"] += 1
                continue
            stats["city: capital"] += 1

        language = ""
        for feed in channel_feeds:
            if feed.get("languages"):
                code = feed["languages"][0]
                language = LANGUAGE_MAP.get(code, code)
                break

        category = "OTHER"
        for name in channel.get("categories") or []:
            if name in CATEGORY_MAP:
                category = CATEGORY_MAP[name]
                break

        tv_rows.append({
            "id": channel["id"],
            "name": channel["name"],
            "countryCode": country,
            "cityId": city_id,
            "language": language,
            "logoUrl": logos.get(channel["id"], ""),
            "description": "",
            "website": channel.get("website") or "",
            "category": category,
            "national": int(national),
        })

        # Сначала потоки основного фида, из них сначала https.
        channel_streams.sort(key=lambda s: (
            s.get("feed") not in (None, main_feed),
            not s["url"].startswith("https://"),
        ))

        seen = set()
        position = 0
        for stream in channel_streams:
            if stream["url"] in seen or position >= max_streams:
                continue
            seen.add(stream["url"])
            stream_rows.append({
                "kind": "tv",
                "itemId": channel["id"],
                "url": stream["url"],
                "quality": stream.get("quality"),
                "userAgent": stream.get("user_agent"),
                "referrer": stream.get("referrer"),
                "position": position,
            })
            position += 1

    # Общенациональных каналов (без города, привязаны к столице) оставляем
    # не больше per_country на страну. Данных о популярности в iptv-org нет,
    # поэтому отбор — по косвенным признакам: проверенный рабочий поток,
    # логотип, известная категория, сайт, число потоков.
    def rank(row):
        urls = [s["url"] for s in streams_by_channel[row["id"]]]
        return (
            not any(url in status for url in urls),
            not row["logoUrl"],
            row["category"] == "OTHER",
            not row["website"],
            -len(urls),
            row["name"].lower(),
        )

    national_by_country = collections.defaultdict(list)
    for row in tv_rows:
        if row["national"]:
            national_by_country[row["countryCode"]].append(row)

    dropped = set()
    for country, rows in sorted(national_by_country.items()):
        rows.sort(key=rank)
        # 0 — без лимита (решение пользователя 2026-11-05: лимит ТВ убран,
        # отбор по косвенным признакам терял ZDF, VOX, Cartoon Network).
        limit = per_country if per_country > 0 else len(rows)
        dropped.update(row["id"] for row in rows[limit:])
        if len(rows) > limit:
            log("tv limit %s: kept %d of %d, first dropped: %s" % (
                country, limit, len(rows), ", ".join(row["name"] for row in rows[limit:limit + 5])
            ))

    stats["skipped: over country limit"] = len(dropped)

    tv_rows = [row for row in tv_rows if row["id"] not in dropped]
    stream_rows = [row for row in stream_rows if row["itemId"] not in dropped]

    return tv_rows, stream_rows, stats


# ------------------------------------------------------------------- радио

def fetch_radio_country(country, limit, refresh, suffix=""):
    """Самые популярные рабочие станции страны из Radio Browser (с кэшем)."""

    path = os.path.join(RADIO_CACHE_DIR, country + suffix + ".json")

    if refresh or not os.path.exists(path):
        os.makedirs(RADIO_CACHE_DIR, exist_ok=True)
        query = ("/json/stations/bycountrycodeexact/%s"
                 "?hidebroken=true&order=clickcount&reverse=true&limit=%d"
                 % (country, limit))
        data = None
        last_error = None
        for host in RADIO_API_HOSTS:
            try:
                request = urllib.request.Request(
                    "https://" + host + query,
                    headers={"User-Agent": "WorldGarden/1.0", "Accept-Encoding": "gzip"},
                )
                with urllib.request.urlopen(request, timeout=90) as response:
                    data = response.read()
                    if response.headers.get("Content-Encoding") == "gzip":
                        data = gzip.decompress(data)
                json.loads(data)
                break
            except Exception as e:  # сервер недоступен — пробуем следующий
                data = None
                last_error = e
        if data is None:
            log("radio: %s failed: %s" % (country, last_error))
            return []
        with open(path, "wb") as f:
            f.write(data)

    with open(path, encoding="utf-8") as f:
        return json.load(f)


def genre_of(text):
    """Первый жанр из GENRE_RULES, чьё слово есть в тексте; иначе None."""

    for genre, words in GENRE_RULES:
        for word in words:
            # Слово должно начинаться не с середины другого: "rap" не в "trap".
            position = text.find(word)
            while position >= 0:
                if position == 0 or not text[position - 1].isalpha():
                    return genre
                position = text.find(word, position + 1)
    return None


def radio_genre(tags, name=""):
    """
    Жанр станции (RadioGenre) по тегам ("pop,rock,80s"). У большой части
    станций в Radio Browser тегов нет вовсе — тогда жанр берём из названия
    ("Rock FM", "Jazz Radio"). Не нашли ни там, ни там — "OTHER".
    """

    return genre_of(tags.lower()) or genre_of(name.lower()) or "OTHER"


class CountryCheck:
    """
    Проверка, что станция действительно из той страны, которой помечена.

    Страну в Radio Browser вписывают сами пользователи, и ошибаются часто:
    например, все "станции Туркменистана" там оказались турецкими и
    арабскими. Сверить каждую станцию с официальным реестром вещателей
    страны автоматически нельзя, поэтому станция берётся, только если
    у неё есть хотя бы одно подтверждение страны и нет ни одного
    противоречия. Признаки:

      - язык вещания — один из языков страны (или наоборот — чужой);
      - сайт станции в национальном домене этой (или другой) страны;
      - координаты станции рядом с городом этой страны (или далеко);
      - регион станции совпадает с названием города этой страны.

    Строго: настоящая станция без этих данных тоже будет отброшена.
    """

    def __init__(self, index):
        self.index = index

        with open(COUNTRY_LANGUAGES_JSON, encoding="utf-8-sig") as f:
            table = json.load(f)

        self.languages = {k: set(v) for k, v in table["countries"].items()}
        self.language_names = table["languageNames"]

    def station_languages(self, station):
        codes = {c.strip().lower()
                 for c in (station.get("languagecodes") or "").split(",") if c.strip()}
        for name in re.split(r"[,/;]", (station.get("language") or "").lower()):
            code = self.language_names.get(name.strip())
            if code:
                codes.add(code)
        return codes

    def check(self, country, station):
        """Возвращает (берём ли станцию, причина отказа)."""

        confirmed = False

        spoken = self.station_languages(station)
        own = self.languages.get(country, set())

        if spoken:
            common = spoken & own
            # Английский подтверждает страну, только если он в ней основной:
            # в таблице языков он числится почти за каждой страной.
            if common - {"en"} or ("en" in common and country in ENGLISH_COUNTRIES):
                confirmed = True
            elif not common:
                return False, "language"

        host = urllib.parse.urlparse(station.get("homepage") or "").hostname or ""
        domain = host.rsplit(".", 1)[-1].upper() if "." in host else ""
        domain = COUNTRY_FIX.get(domain, domain)

        if (len(domain) == 2 and domain in self.index.capital
                and (domain == country or domain not in VANITY_DOMAINS)):
            if domain != country:
                return False, "domain"
            confirmed = True

        lat, lon = station.get("geo_lat"), station.get("geo_long")

        if lat is not None and lon is not None:
            if self.index.nearest(country, float(lat), float(lon), RADIO_COUNTRY_MAX_KM) is None:
                return False, "coordinates"
            confirmed = True

        if station.get("state") and self.index.find(country, station["state"]) is not None:
            confirmed = True

        return (True, "") if confirmed else (False, "no evidence")


def build_radio(index, per_country, refresh):

    countries = sorted(index.capital)
    checker = CountryCheck(index)
    stats = collections.Counter()

    def select(country, stations, counter):
        """Отбирает до per_country станций страны. Возвращает (станции, потоки)."""

        rows, streams = [], []
        seen_names, seen_urls = set(), set()

        for station in stations:

            if len(rows) >= per_country:
                break

            name = " ".join((station.get("name") or "").split())[:80]
            url = (station.get("url_resolved") or station.get("url") or "").strip()

            if not name or not url.startswith(("http://", "https://")):
                continue

            key = normalize(name)
            if not key or key in seen_names or url in seen_urls:
                continue

            if is_adult_name(name) or ADULT_TAG_RE.search(station.get("tags") or ""):
                counter["radio rejected: adult"] += 1
                continue

            ok, reason = checker.check(country, station)
            if not ok:
                counter["radio rejected: " + reason] += 1
                continue

            seen_names.add(key)
            seen_urls.add(url)

            city_id = None
            lat, lon = station.get("geo_lat"), station.get("geo_long")
            if not CAPITALS_ONLY and lat is not None and lon is not None:
                city_id = index.nearest(country, float(lat), float(lon), RADIO_CITY_MAX_KM)
            if not CAPITALS_ONLY and city_id is None and station.get("state"):
                city_id = index.find(country, station["state"])
            national = city_id is None
            if national:
                city_id = index.capital[country]

            language =(station.get("languagecodes") or "").split(",")[0].strip().lower()
            station_id = "rb-" + station["stationuuid"]
            logo = (station.get("favicon") or "").strip()

            rows.append({
                "id": station_id,
                "name": name,
                "countryCode": country,
                "cityId": city_id,
                "language": LANGUAGE_MAP.get(language, language),
                "logoUrl": logo if logo.startswith(("http://", "https://")) else "",
                "description": "",
                "website": (station.get("homepage") or "").strip(),
                "genre": radio_genre(station.get("tags") or "", name),
                "national": int(national),
            })

            streams.append({
                "kind": "radio",
                "itemId": station_id,
                "url": url,
                "quality": ("%s kbps" % station["bitrate"]) if station.get("bitrate") else None,
                "userAgent": None,
                "referrer": None,
                "position": 0,
            })

        return rows, streams

    # Страны запрашиваются параллельно: запросов много, но каждый маленький.
    def fetch_all(names, limit, suffix):
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            return dict(zip(names, pool.map(
                lambda country: fetch_radio_country(country, limit, refresh, suffix), names
            )))

    base = fetch_all(countries, RADIO_BASE_LIMIT, "")

    # Где после проверки не набралось per_country, а кандидаты ещё есть
    # (ответ упёрся в лимит) — один раз берём список длиннее.
    need_more = [
        country for country in countries
        if len(base[country]) >= RADIO_BASE_LIMIT
        and len(select(country, base[country], collections.Counter())[0]) < per_country
    ]

    more = fetch_all(need_more, RADIO_MORE_LIMIT, ".more")

    # Если и длинный список упёрся в лимит, а станций всё ещё мало —
    # берём самый длинный. Так большой запрос уходит только по тем
    # странам, где он действительно нужен.
    need_most = [
        country for country in need_more
        if len(more[country]) >= RADIO_MORE_LIMIT
        and len(select(country, more[country], collections.Counter())[0]) < per_country
    ]

    most = fetch_all(need_most, RADIO_MOST_LIMIT, ".most")

    radio_rows, stream_rows = [], []
    confirmed_countries = 0

    for country in countries:
        stations = most.get(country) or more.get(country) or base[country]
        rows, streams = select(country, stations, stats)
        radio_rows.extend(rows)
        stream_rows.extend(streams)
        stats["radio candidates"] += len(stations)
        if rows:
            confirmed_countries += 1

    # Ручные дополнения: станции, которых нет в Radio Browser, с адресом
    # потока из открытого официального источника (поле "source").
    if os.path.exists(RADIO_EXTRA_JSON):
        with open(RADIO_EXTRA_JSON, encoding="utf-8") as f:
            extras = json.load(f)

        known = {row["id"] for row in radio_rows}

        for extra in extras:
            country = extra["countryCode"]
            city_id = (
                (None if CAPITALS_ONLY else extra.get("cityId"))
                or index.capital.get(country)
            )
            if extra["id"] in known or city_id is None:
                continue
            radio_rows.append({
                "id": extra["id"],
                "name": extra["name"],
                "countryCode": country,
                "cityId": city_id,
                "language": extra.get("language", ""),
                "logoUrl": extra.get("logoUrl", ""),
                "description": "",
                "website": extra.get("website", ""),
                "genre": extra.get("genre", "OTHER"),
                "national": int(CAPITALS_ONLY or not extra.get("cityId")),
            })
            stream_rows.append({
                "kind": "radio",
                "itemId": extra["id"],
                "url": extra["url"],
                "quality": None,
                "userAgent": None,
                "referrer": None,
                "position": 0,
            })
            stats["radio added by hand"] += 1

    stats["radio countries with stations"] = confirmed_countries
    stats["radio countries refetched"] = len(need_more)

    return radio_rows, stream_rows, stats


# ---------------------------------------------------------------------- база

def insert(db, table, rows):
    if not rows:
        return
    columns = list(rows[0].keys())
    sql = "INSERT INTO `%s` (%s) VALUES (%s)" % (
        table,
        ", ".join("`%s`" % c for c in columns),
        ", ".join("?" for _ in columns),
    )
    db.executemany(sql, [[row[c] for c in columns] for row in rows])


def create_schema(db):
    """Создаёт таблицы точно так, как их ждёт Room (по выгруженной схеме)."""

    if not os.path.exists(SCHEMA_JSON):
        sys.exit("Нет схемы Room: %s\nСначала соберите проект: gradlew.bat assembleDebug"
                 % SCHEMA_JSON)

    with open(SCHEMA_JSON, encoding="utf-8") as f:
        schema = json.load(f)["database"]

    for entity in schema["entities"]:
        table = entity["tableName"]
        db.execute(entity["createSql"].replace("${TABLE_NAME}", table))
        for idx in entity.get("indices", []):
            db.execute(idx["createSql"].replace("${TABLE_NAME}", table))

    # Таблица Room с хэшем схемы: с ней Room принимает файл без проверок.
    for query in schema["setupQueries"]:
        db.execute(query)

    db.execute("PRAGMA user_version = %d" % schema["version"])


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--max-streams", type=int, default=5)
    parser.add_argument("--radio-per-country", type=int, default=300)
    parser.add_argument("--tv-per-country", type=int, default=0)
    args = parser.parse_args()

    with open(CITIES_JSON, encoding="utf-8") as f:
        cities = json.load(f)

    index = CityIndex(cities)

    tv_rows, tv_streams, stats = build_tv(
        index, args.max_streams, args.tv_per_country, args.refresh
    )

    radio_rows, radio_streams, radio_stats = build_radio(
        index, args.radio_per_country, args.refresh
    )
    stats.update(radio_stats)

    # id потоков сквозные: радио продолжает нумерацию после ТВ.
    stream_rows = [
        {"id": number, **stream}
        for number, stream in enumerate(tv_streams + radio_streams, start=1)
    ]

    city_rows = [{
        "id": c["id"],
        "name": c["name"],
        "countryCode": c["countryCode"],
        "latitude": c["latitude"],
        "longitude": c["longitude"],
        "timezone": c.get("timezone", ""),
        "population": c.get("population", 0),
        "capital": 1 if c.get("capital") else 0,
    } for c in cities]

    # Собираем во временный файл и подменяем готовым: если сборка упадёт,
    # прежняя база каталога останется на месте.
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    temp_db = OUTPUT_DB + ".tmp"
    if os.path.exists(temp_db):
        os.remove(temp_db)

    db = sqlite3.connect(temp_db)
    try:
        create_schema(db)
        insert(db, "cities", city_rows)
        insert(db, "tv_channels", tv_rows)
        insert(db, "radio_stations", radio_rows)
        insert(db, "streams", stream_rows)
        db.commit()
        db.execute("VACUUM")
    finally:
        db.close()

    os.replace(temp_db, OUTPUT_DB)

    with open(SCHEMA_JSON, encoding="utf-8") as f:
        identity = json.load(f)["database"]["identityHash"]
    with open(OUTPUT_INFO, "w", encoding="utf-8") as f:
        json.dump({
            # ГГГГММДДЧЧММ: две сборки за час не должны получить один номер.
            "version": int(datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M")),
            "identityHash": identity,
        }, f)

    for key in sorted(stats):
        log("%-30s %d" % (key, stats[key]))

    by_city = collections.Counter(row["cityId"] for row in tv_rows)
    names = {c["id"]: c["name"] for c in cities}

    log()
    log("cities         ", len(city_rows))
    log("tv_channels    ", len(tv_rows))
    log("streams        ", len(stream_rows))
    log("radio_stations ", len(radio_rows))
    log("radio with logo", sum(1 for row in radio_rows if row["logoUrl"]))
    log("cities with tv ", len(by_city))
    log("cities w/ radio", len({row["cityId"] for row in radio_rows}))
    log("largest lists  ", ", ".join(
        "%s %d" % (names[city_id], count) for city_id, count in by_city.most_common(6)
    ))
    log()
    log("%s  %.1f MB" % (os.path.relpath(OUTPUT_DB, ROOT), os.path.getsize(OUTPUT_DB) / 1e6))


if __name__ == "__main__":
    main()
