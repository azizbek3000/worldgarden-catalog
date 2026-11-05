#!/usr/bin/env python3
"""
Готовит каталог к раздаче: сжимает catalog.db и пишет manifest.json.

Запускается после tools/build_catalog.py — на компьютере или в GitHub
Actions (.github/workflows/build-catalog.yml репозитория каталога).
Приложение сначала читает маленький manifest.json и качает файл, только
если версия новее его каталога.

manifest.json:
    version       номер сборки (ГГГГММДДЧЧММ по UTC; старые — ГГГГММДДЧЧ), растёт с каждой сборкой
    identityHash  хэш схемы Room: приложение берёт файл, только если он
                  совпадает с его схемой (иначе нужна новая версия приложения)
    file          имя сжатого файла рядом с манифестом
    gzSize        размер сжатого файла, байт
    size, sha256  размер и SHA-256 распакованной базы — проверка целостности
    tvChannels, radioStations, streams — для журнала

Запуск из корня проекта:

    py tools/publish_catalog.py               # в tools/cache/site
    py tools/publish_catalog.py --out site
"""

import argparse
import datetime
import gzip
import hashlib
import json
import os
import shutil
import sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CATALOG_DB = os.path.join(ROOT, "app", "src", "main", "assets", "databases", "catalog.db")
CATALOG_INFO = os.path.join(ROOT, "app", "src", "main", "assets", "databases", "catalog.json")


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", default=os.path.join(ROOT, "tools", "cache", "site"))
    args = parser.parse_args()

    db = sqlite3.connect(CATALOG_DB)
    try:
        identity = db.execute(
            "SELECT identity_hash FROM room_master_table WHERE id = 42"
        ).fetchone()[0]
        counts = {
            table: db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
            for table in ("tv_channels", "radio_stations", "streams")
        }
    finally:
        db.close()

    # Номер сборки пишет build_catalog.py; без файла — текущий час.
    version = int(datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M"))
    if os.path.exists(CATALOG_INFO):
        with open(CATALOG_INFO, encoding="utf-8") as f:
            version = json.load(f)["version"]
    name = "catalog-%d.db.gz" % version

    os.makedirs(args.out, exist_ok=True)
    packed = os.path.join(args.out, name)
    with open(CATALOG_DB, "rb") as src, gzip.open(packed, "wb", compresslevel=9) as dst:
        shutil.copyfileobj(src, dst)

    manifest = {
        "version": version,
        "identityHash": identity,
        "file": name,
        "gzSize": os.path.getsize(packed),
        "size": os.path.getsize(CATALOG_DB),
        "sha256": sha256(CATALOG_DB),
        "tvChannels": counts["tv_channels"],
        "radioStations": counts["radio_stations"],
        "streams": counts["streams"],
    }

    with open(os.path.join(args.out, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
