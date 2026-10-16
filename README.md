# Зеркало шаблонов плейлистов от epg.one с обновленными ссылками для ProIPTV и IPTV Channels Editor

Обновление происходит раз в 3 дня.

## Ссылки на зеркало

Укажите в приложении один из следующих адресов вместо оригинального плейлиста:

```
https://raw.githubusercontent.com/KocourKuba/epgone_mirror/master/playlists/edem_epg_ico.m3u8
https://raw.githubusercontent.com/KocourKuba/epgone_mirror/master/playlists/edem_epg_ico2.m3u8
https://raw.githubusercontent.com/KocourKuba/epgone_mirror/master/playlists/edem_epg_ico3.m3u8
```

Ссылки `tvg-logo` внутри плейлистов ведут на квадратные иконки в каталоге `img/`.
Прямоугольные версии тех же иконок (`<id>.png`) лежат в каталоге `img2/`.

## Запуск вручную

Обновление выполняется автоматически через GitHub Actions. При необходимости можно запустить вручную:

- через Actions: вкладка **Actions** → **Mirroring Playlists** → **Run workflow**;
- либо локально:

```
pip install requests
python mirror_playlists.py
```
