# worldgarden-catalog

Каталог ТВ-каналов и радиостанций для приложения World Garden.

Раз в месяц GitHub Actions собирает базу из открытых источников
(iptv-org, Radio Browser), отсеивает нерабочие потоки и публикует
`manifest.json` и `catalog-<версия>.db.gz` на GitHub Pages.

Файлы этого репозитория копирует `tools/prepare_catalog_repo.py` из
проекта приложения — правьте их там.
