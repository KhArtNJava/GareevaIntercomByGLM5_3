// intercom-player.js — живое воспроизведение H.264 от видеодомофона.
// Оба канала: jmuxer (Annex-B → fMP4 → MSE <video>).
//   Канал 0: субпоток 704×576 — устройство шлёт SPS/PPS с каждым IDR, играет как есть.
//   Канал 1 (HD 720p25): P-only поток устройства с intra-refresh убивает строгие
//   декодеры Chrome (WebCodecs «Decoding error», MSE «Failed to send video packet
//   for decoding») — сервер транскодирует его через FFmpeg в чистый x264 Baseline
//   с AUD и IDR каждые 50 кадров (см. Ch1Transcoder.cs), клиенту не нужно знать
//   об этом: поток приходит уже в «обычном» формате.
// Общий протокол /videofeed/{ch}: [4Б duration мс][4Б длина][кадр Annex-B].
(function () {
    'use strict';

    let active = null;       // текущая сессия start/stop
    let activeMuxer = null;  // единственный живой JMuxer (сериализация MSE)

    // Гарантированно уничтожить текущий JMuxer. Должно быть СИНХРОННЫМ:
    // если старый MediaSource ещё открыт на <video>, создание нового
    // JMuxer на том же элементе даёт InvalidStateError («buffer error»).
    function killMuxer() {
        if (activeMuxer) {
            try { activeMuxer.destroy(); } catch (e) { /* уже мёртв */ }
            activeMuxer = null;
        }
    }

    // ---------- воспроизведение: jmuxer + MSE ----------
    async function runMse(video, overlay, channel, signal) {
        overlay.hidden = false;
        overlay.textContent = 'Подключение к домофону…';

        killMuxer(); // старая MSE-сессия закрыта ДО создания новой

        let muxerError = null;
        const muxer = new JMuxer({
            node: video.id,
            mode: 'video',
            flushingTime: 0,
            fps: 25,
            debug: false,
            onReady: function () { overlay.hidden = true; },
            onError: function (err) {
                console.warn('jmuxer:', err);
                muxerError = (err && err.error) ? err.error : (err || 'buffer error');
            }
        });
        activeMuxer = muxer;

        let feedCount = 0;
        try {
            const resp = await fetch('/videofeed/' + channel, { signal: signal });
            if (!resp.ok || !resp.body) throw new Error('HTTP ' + resp.status);

            overlay.textContent = 'Получение видеоданных…';
            const reader = resp.body.getReader();

            let buf = new Uint8Array(0);
            let started = false;

            while (true) {
                const { done, value } = await reader.read();
                if (done || signal.aborted) break;

                const nb = new Uint8Array(buf.length + value.length);
                nb.set(buf, 0);
                nb.set(value, buf.length);
                buf = nb;

                while (buf.length >= 8) {
                    const dur = buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
                    const len = buf[4] | (buf[5] << 8) | (buf[6] << 16) | (buf[7] << 24);
                    if (buf.length < 8 + len) break;

                    const frame = buf.subarray(8, 8 + len);
                    buf = buf.slice(8 + len);

                    if (muxerError) throw new Error('MSE: ' + muxerError);

                    if (!started) {
                        if (findStartCode(frame, 7) < 0) continue; // ждём SPS (NAL 7)
                        started = true;
                    }
                    muxer.feed({ video: new Uint8Array(frame), duration: dur > 0 ? dur : 40 });
                    feedCount++;
                }
            }
        } finally {
            if (activeMuxer === muxer) killMuxer();
        }
        return feedCount;
    }

    // Поиск Annex-B start code перед NAL заданного типа (7=SPS, 8=PPS, 5=IDR…)
    function findStartCode(u8, nalType) {
        for (let i = 0; i < u8.length - 4; i++) {
            if (u8[i] === 0 && u8[i + 1] === 0 && u8[i + 2] === 1 && (u8[i + 3] & 0x1f) === nalType) return i;
            if (u8[i] === 0 && u8[i + 1] === 0 && u8[i + 2] === 0 && u8[i + 3] === 1 && (u8[i + 4] & 0x1f) === nalType) return i;
        }
        return -1;
    }

    window.intercomPlayer = {
        // Запуск воспроизведения. videoId — id <video>, overlayId — id плашки статуса.
        start: async function (videoId, overlayId, channel) {
            const video = document.getElementById(videoId);
            const overlay = document.getElementById(overlayId);
            if (!video || !overlay) return 'not found';

            this.stop();

            const ctl = { abort: new AbortController(), stopped: false };
            active = ctl;

            while (!ctl.stopped) {
                try {
                    const frames = await runMse(video, overlay, channel, ctl.abort.signal);
                    if (ctl.stopped) break;
                    throw new Error('поток завершён (' + frames + ' кадров)');
                } catch (e) {
                    if (ctl.stopped || e.name === 'AbortError') break;
                    console.error('Поток прерван:', e);
                    overlay.hidden = false;
                    overlay.textContent = 'Ошибка: ' + (e.message || e) + ' — повтор через 2 с…';
                }
                await new Promise(r => setTimeout(r, 2000));
                if (ctl.stopped) break;
                overlay.hidden = false;
                overlay.textContent = 'Переподключение…';
            }
            return 'ok';
        },

        stop: function () {
            if (active) {
                active.stopped = true;
                active.abort.abort();
                active = null;
            }
            // Не ждём асинхронного AbortError из fetch — закрываем MSE сразу,
            // иначе новый start() создаст второй MediaSource на том же <video>.
            killMuxer();
        }
    };
})();
