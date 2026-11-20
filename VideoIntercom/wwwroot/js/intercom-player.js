// intercom-player.js — живое воспроизведение H.264 от видеодомофона.
// Тянет /videofeed/{channel} с кадрированием [4Б duration][4Б длина][кадр],
// передаёт NAL-юниты в jmuxer (Annex-B → fMP4 → MSE <video>).
// duration каждого кадра берётся из µs-таймстампов домофона,
// поэтому MSE-таймлайн совпадает с реальным темпом съёмки.
(function () {
    'use strict';

    let active = null;

    async function run(video, overlay, channel, signal) {
        overlay.hidden = false;
        overlay.textContent = 'Подключение к домофону…';

        const muxer = new JMuxer({
            node: video.id,
            mode: 'video',
            flushingTime: 0,
            fps: 25,              // заявленная частота; фактический темп задаёт duration кадров
            debug: false,
            onReady: function () { overlay.hidden = true; },
            onError: function (err) { console.warn('jmuxer:', err); }
        });

        const resp = await fetch('/videofeed/' + channel, { signal: signal });
        if (!resp.ok || !resp.body) throw new Error('HTTP ' + resp.status);

        overlay.textContent = 'Получение видеоданных…';
        const reader = resp.body.getReader();

        // Кадрирование: [4Б duration мс][4Б длина][кадр]
        let buf = new Uint8Array(0);
        let started = false;      // ждём первый IDR (со SPS/PPS), до него MSE не кормим
        let lastTs = performance.now();
        let feedCount = 0;

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
                if (buf.length < 8 + len) break; // кадр не докачан

                const frame = buf.subarray(8, 8 + len);
                buf = buf.slice(8 + len);

                if (!started) {
                    // Ищем SPS (NAL 7) в кадре: маркер 00 00 01 67 или 00 00 00 01 67
                    const s = findStartCode(frame, 7);
                    if (s < 0) continue; // пропускаем P-кадры до первого IDR
                    started = true;
                }

                // Реальный темп: duration в мс от домофона (0 только у первого кадра)
                const duration = dur > 0 ? dur : 40;
                muxer.feed({ video: toCopy(frame), duration: duration });
                feedCount++;
                lastTs = performance.now();
            }
        }
        muxer.destroy();
        return feedCount;
    }

    function toCopy(u8) { // subarray над общим буфером копируем — jmuxer владеет данными
        return new Uint8Array(u8);
    }

    function findStartCode(u8, nalType) {
        for (let i = 0; i < u8.length - 4; i++) {
            if (u8[i] === 0 && u8[i + 1] === 0 && u8[i + 2] === 1 && (u8[i + 3] & 0x1f) === nalType) return i;
            if (u8[i] === 0 && u8[i + 1] === 0 && u8[i + 2] === 0 && u8[i + 3] === 1 && (u8[i + 4] & 0x1f) === nalType) return i;
        }
        return -1;
    }

    window.intercomPlayer = {
        // Запуск воспроизведения. videoId — id <video>, overlayId — id плашки статуса.
        start: async function (videoId, overlayId, channel, fps) {
            const video = document.getElementById(videoId);
            const overlay = document.getElementById(overlayId);
            if (!video || !overlay) return 'not found';

            this.stop();

            const ctl = { abort: new AbortController(), stopped: false };
            active = ctl;

            while (!ctl.stopped) {
                try {
                    await run(video, overlay, channel ?? 0, ctl.abort.signal);
                    if (ctl.stopped) break;
                    throw new Error('поток завершён');
                } catch (e) {
                    if (ctl.stopped || e.name === 'AbortError') break;
                    console.error('Поток прерван:', e);
                    overlay.hidden = false;
                    overlay.textContent = 'Ошибка: ' + e.message + ' — повтор через 2 с…';
                }
                await new Promise(r => setTimeout(r, 2000));
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
        }
    };
})();
