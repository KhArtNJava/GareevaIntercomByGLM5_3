# Архитектура приложения VideoIntercom

Blazor Web App (.NET 10, интерактивный режим Server) для живого просмотра
видеопотока домофона в браузере.

```
Домофон ──TCP:9000──► IntercomStreamService ──кадры+duration──► /videofeed/{ch}
   (MDVR-протокол)      (BackgroundService,                    (Minimal API,
                         singleton, автопереподключение)        chunked-stream)
                                                                        │
                                           fetch + разбор кадров      │
                                           ┌──────────────────────────┘
                                           ▼
               <video> ◄── MediaSource ◄── jmuxer (Annex-B → fMP4)
```

## Компоненты

### `IntercomStreamService.cs` — источник потока

- `BackgroundService`-синглтон: подключается к `192.168.31.150:9000`,
  шлёт handshake (`toServer720.bin`, вшит как base64-литерал),
  читает и разбирает поток (формат — `docs/PROTOCOL.md`).
- Разбор потоковый, чанками по 64 КБ с переносом хвоста: кадр публикуется,
  когда в буфере появился следующий маркер (граница кадра известна).
- **Длительность кадра** вычисляется из µs-таймстампов соседних пакетов
  того же канала (`u64 LE` на +16 от маркера) — это основа честного тайминга.
- Подписчики — по каналам, очередь на каждого (bounded 600, DropOldest:
  живому видео лучше пропускать, чем копить).
- При обрыве связи — переподключение через 3 с; статус доступен в
  `Status`, `FramesTotal`, `LastFrameUtc` (используются в UI и `/videostatus`).

### `Program.cs` — эндпоинты

| Маршрут | Назначение |
|---|---|
| `GET /videofeed/{channel}` | живой поток кадров; channel: 0 (рабочий) или 1 |
| `GET /videostatus` | JSON: статус соединения, счётчик кадров, время последнего |

**Контракт `/videofeed`** — последовательность записей:

```
[4 байта duration, i32 LE, мс][4 байта length, i32 LE][кадр Annex-B H.264]
```

MIME `application/octet-stream`, `Cache-Control: no-store`. Стрим бесконечный
(chunked); завершается при обрыве связи с домофоном или с клиентом.

### `Components/Pages/Home.razor` — страница

- `<video muted autoplay playsinline>` + оверлей статуса;
- `@rendermode InteractiveServer` обязателен (иначе `OnAfterRender`
  и JS-интероп не выполняются);
- таймер 1 с обновляет бейдж статуса/счётчик кадров;
- скрипты подключены в `App.razor` (не в компоненте — Blazor не гарантирует
  исполнение `<script>` внутри компонента после рендера).

### `wwwroot/js/intercom-player.js` — клиентский плеер

- тянет `/videofeed/{ch}` через `fetch` + `ReadableStream`;
- разбирает кадрирование, **ждёт первый IDR со SPS/PPS** (NAL type 7),
  до него кадры в MSE не отдаются;
- кормит `JMuxer.feed({video, duration})` — jmuxer пакует NAL-юниты в fMP4
  и передаёт в `<video>` через MediaSource Extensions;
- авто-переподключение с интервалом 2 с; методы `intercomPlayer.start/stop`.

### `wwwroot/lib/jmuxer/dist/` — jmuxer 2.0.5 (UMD)

Глобальное имя — **`JMuxer`** (с большой буквы; UMD-экспорт при отсутствии
`module`/`define` пишет его в `globalThis.JMuxer`).

## Выбор канала

Канал 0 (704×576, ~1.25 fps) — единственный самодостаточный: SPS/PPS
приходят с каждым IDR, поток можно начинать читать с любой точки.
Канал 1 (720p, 25 fps) не декодируется — заголовки не транслируются
(см. `docs/PROTOCOL.md`, раздел 5).

## Конфигурация и запуск

```bash
cd VideoIntercom
dotnet run --urls http://*:5244     # http://localhost:5244
```

Адрес домофона задан константами `DefaultHost`/`DefaultPort`
в `IntercomStreamService.cs` (192.168.31.150:9000).

## Отладка

```bash
# статус источника
curl http://localhost:5244/videostatus

# сырой поток: проверить кадрирование
curl --max-time 5 -N http://localhost:5244/videofeed/0 | xxd | head

# декодирование потока сервера в кадры (сквозная проверка)
curl --max-time 10 -N http://localhost:5244/videofeed/0 | \
    python3 -c "import sys,struct; d=sys.stdin.buffer.read(); p=0
while p+8<=len(d):
    dur,ln=struct.unpack_from('<ii',d,p); sys.stdout.buffer.write(d[p+8:p+8+ln]); p+=8+ln" | \
    ffmpeg -f h264 -i - -frames:v 1 check.jpg
```

Headless-тест страницы (Playwright + системный Chrome):

```bash
python3 browser_test.py   # см. /tmp/opencode/browser_test2.py в истории сессии
```

## Известные ограничения

- Задержка старта ~1 с: первый показ возможен только после первого IDR
  (заголовки потока приходят только с ключевыми кадрами).
- Канал 1 (720p) не задействован — нет SPS/PPS (задача на будущее).
- Один общий handshake на сессию; множественные зрители одного канала
  получают кадры из одной TCP-связи с домофоном (рассылка подписчикам).
- Браузер без MSE (старые Safari/IE) не поддерживаются.
