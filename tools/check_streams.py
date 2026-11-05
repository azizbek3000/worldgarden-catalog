#!/usr/bin/env python3
"""
Проверяет, какие потоки iptv-org отвечают (PHASE 9), и пишет результат
в tools/cache/stream_status.json. tools/build_catalog.py по этому файлу
выбрасывает нерабочие потоки и каналы, у которых не осталось ни одного.

Проверка простая: запрашиваем адрес и читаем начало ответа. Поток
считается живым, если сервер ответил 2xx и это похоже на плейлист
(HLS / DASH) или на медиаданные. Само видео не скачивается.

Важно: проверка идёт из сети этого компьютера. Поток, закрытый для
вашей страны или провайдера, будет помечен нерабочим, хотя в другой
сети он может играть.

Трафик: на каждый поток уходит одно соединение и около килобайта
ответа, но само установление тысяч защищённых соединений тоже весит
немало. Уже проверенные потоки повторно не запрашиваются — без
--recheck скрипт проверяет только новые.

Запуск из корня проекта:

    py tools/check_streams.py            # проверить ещё не проверенные
    py tools/check_streams.py --recheck  # проверить все заново

Параметры:

    --recheck      игнорировать прошлые результаты
    --workers N    сколько потоков проверять одновременно (по умолчанию 40)
    --timeout S    сколько секунд ждать ответа (по умолчанию 10)
"""

import argparse
import concurrent.futures
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STREAMS_JSON = os.path.join(ROOT, "tools", "cache", "iptv", "streams.json")
STATUS_JSON = os.path.join(ROOT, "tools", "cache", "stream_status.json")

DEFAULT_UA = (
    "Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0 Mobile Safari/537.36"
)

# Сертификаты не проверяем: у многих вещателей они просрочены, а плееру
# это решать самому — нам важно только, отвечает ли адрес.
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

# Сколько байт ответа читаем: этого хватает, чтобы узнать плейлист или видео.
READ_BYTES = 1024

MEDIA_TYPES =("video/", "audio/", "application/vnd.apple.mpegurl", "application/x-mpegurl",
               "application/dash+xml", "application/octet-stream", "binary/octet-stream")


def check(stream, timeout):
    """Возвращает (ok, причина)."""

    url = stream["url"]

    if not url.startswith(("http://", "https://")):
        return False, "scheme"

    headers = {"User-Agent": stream.get("user_agent") or DEFAULT_UA}
    if stream.get("referrer"):
        headers["Referer"] = stream["referrer"]

    def fetch(extra):
        request = urllib.request.Request(url, headers={**headers, **extra})
        with urllib.request.urlopen(request, timeout=timeout, context=SSL_CONTEXT) as response:
            return (
                response.read(READ_BYTES),
                (response.headers.get("Content-Type") or "").lower(),
            )

    try:
        try:
            # Экономия трафика: просим у сервера только первый килобайт,
            # чтобы он не начинал отдавать само видео.
            head, content_type = fetch({"Range": "bytes=0-%d" % (READ_BYTES - 1)})
        except urllib.error.HTTPError as e:
            if e.code != 416:
                raise
            # Сервер не умеет отдавать часть — запрашиваем обычным способом.
            head, content_type = fetch({})
    except urllib.error.HTTPError as e:
        return False, "http %d" % e.code
    except Exception as e:  # таймауты, DNS, обрывы, кривые адреса
        return False, type(e).__name__

    text = head.lstrip()[:200].lower()

    if text.startswith(b"#extm3u") or b"<mpd" in head.lower():
        return True, "playlist"
    if text.startswith((b"<!doctype html", b"<html")):
        return False, "html"
    if content_type.startswith(MEDIA_TYPES) and head:
        return True, "media"
    if head[:1] == b"G":  # MPEG-TS начинается с байта синхронизации 0x47
        return True, "media"

    return False, "unknown content"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--recheck", action="store_true")
    parser.add_argument("--workers", type=int, default=40)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()

    with open(STREAMS_JSON, encoding="utf-8") as f:
        streams = [s for s in json.load(f) if s.get("channel") and s.get("url")]

    status = {}
    if not args.recheck and os.path.exists(STATUS_JSON):
        with open(STATUS_JSON, encoding="utf-8") as f:
            status = json.load(f)

    todo = {}
    for stream in streams:
        if stream["url"] not in status:
            todo.setdefault(stream["url"], stream)

    print("streams %d, already checked %d, to check %d"
          % (len(streams), len(status), len(todo)), flush=True)

    def save():
        tmp = STATUS_JSON + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False)
        os.replace(tmp, STATUS_JSON)

    started = time.time()
    done = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(check, s, args.timeout): url for url, s in todo.items()}
        for future in concurrent.futures.as_completed(futures):
            ok, reason = future.result()
            status[futures[future]] = {"ok": ok, "reason": reason}
            done += 1
            if done % 500 == 0:
                save()
                alive = sum(1 for v in status.values() if v["ok"])
                print("%d/%d checked, alive so far %d, %.0f s"
                      % (done, len(todo), alive, time.time() - started), flush=True)

    save()

    alive = sum(1 for v in status.values() if v["ok"])
    reasons = {}
    for value in status.values():
        if not value["ok"]:
            reasons[value["reason"]] = reasons.get(value["reason"], 0) + 1

    print()
    print("alive %d of %d" % (alive, len(status)))
    for reason, count in sorted(reasons.items(), key=lambda item: -item[1])[:12]:
        print("  dead: %-18s %d" % (reason, count))
    print(os.path.relpath(STATUS_JSON, ROOT))


if __name__ == "__main__":
    sys.exit(main())
