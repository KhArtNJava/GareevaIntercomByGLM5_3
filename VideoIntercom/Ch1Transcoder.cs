using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;

namespace VideoIntercom;

/// <summary>
/// Транскодер канала 1 (720p25).
///
/// Основной поток домофона — P-only с intra-refresh и сбросами frame_num без IDR.
/// Аппаратный энкодер устройства при этом слегка неспецификационен (intra-режимы
/// в строке 0 макроблоков): строгие декодеры Chrome — и WebCodecs VideoDecoder,
/// и MSE-пайплайн — фатально прерываются на таких P-слайсах
/// («PIPELINE_ERROR_DECODE: Failed to send video packet for decoding»).
/// FFmpeg тот же поток декодирует с error concealment (проверено: сотни кадров,
/// сходимость ~1 GOP).
///
/// Поэтому канал 1 прогоняется через FFmpeg:
///   decode (concealment) → re-encode x264 Baseline, AUD, IDR каждые 50 кадров —
/// и браузер получает обычный чистый поток, который MSE переваривает как канал 0.
///
/// Жизненный цикл: сессия FFmpeg запускается при первом подписчике /videofeed/1
/// и останавливается, когда подписчиков не остаётся (экономия CPU). При падении
/// FFmpeg подписчики закрываются — клиенты переподключаются и поднимают новую сессию.
/// </summary>
public sealed class Ch1TranscoderManager
{
    private readonly ILogger<Ch1TranscoderManager> _logger;
    private readonly IntercomStreamService _source;
    private readonly object _gate = new();
    private Session? _session;
    private int _refs;

    public Ch1TranscoderManager(ILogger<Ch1TranscoderManager> logger, IntercomStreamService source)
    {
        _logger = logger;
        _source = source;
    }

    /// <summary>Взять подписку на транскоднутый поток (лениво запускает FFmpeg).</summary>
    public IDisposable Acquire(out ChannelReader<IntercomStreamService.VideoFrame> reader)
    {
        lock (_gate)
        {
            if (_session is { Completed: true })
            {
                _session.Shutdown("stale");
                _session = null;
                _refs = 0;
            }
            _session ??= new Session(this);
            _refs++;
        }
        return _session.Subscribe(out reader);
    }

    internal void Release(object session)
    {
        if (session is not Session s || _session != s) return;
        lock (_gate)
        {
            if (_session != s) return;
            if (--_refs > 0) return;
            _session = null;
            _refs = 0;
        }
        s.Shutdown("нет подписчиков");
    }

    // ---------------------------------------------------------------- Session

    private sealed class Session : IDisposable
    {
        private readonly Ch1TranscoderManager _owner;
        private readonly ConcurrentDictionary<Guid, Channel<IntercomStreamService.VideoFrame>> _subs = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly IDisposable _sourceSub;
        private readonly Process _ffmpeg;
        private readonly Task _pumpIn;
        private readonly Task _pumpOut;
        private readonly Task _pumpErr;
        private volatile bool _completed;

        public bool Completed => _completed;

        public Session(Ch1TranscoderManager owner)
        {
            _owner = owner;
            var psi = new ProcessStartInfo
            {
                FileName = "ffmpeg",
                Arguments =
                    "-hide_banner -loglevel error -fflags +genpts " +
                    "-f h264 -framerate 25 -i pipe:0 " +
                    "-an -c:v libx264 -profile:v baseline -preset ultrafast -tune zerolatency " +
                    "-x264-params aud=1:keyint=50:min-keyint=50:scenecut=0 " +
                    "-pix_fmt yuv420p -f h264 pipe:1",
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            _ffmpeg = Process.Start(psi)
                ?? throw new InvalidOperationException("не удалось запустить ffmpeg");

            // Питание: сырой канал 1 → stdin FFmpeg.
            // Сначала синтезированные SPS/PPS и серый IDR — декодер стартует
            // с нуля, дальше P-слайсы устройства (сходимость ~1 GOP).
            _sourceSub = _owner._source.Subscribe(1, out var src);
            _pumpIn = Task.Run(() => PumpInputAsync(src));
            _pumpOut = Task.Run(PumpOutputAsync);
            _pumpErr = Task.Run(PumpErrAsync);

            _owner._logger.LogInformation("Ch1 transcoder started (pid {Pid})", _ffmpeg.Id);
        }

        private async Task PumpErrAsync()
        {
            try
            {
                var text = await _ffmpeg.StandardError.ReadToEndAsync(_cts.Token);
                if (!string.IsNullOrWhiteSpace(text))
                    _owner._logger.LogWarning("Ch1 ffmpeg stderr: {Text}", text.Trim());
            }
            catch (OperationCanceledException) { }
            catch (Exception) { /* процесс умер — не важно */ }
        }

        public IDisposable Subscribe(out ChannelReader<IntercomStreamService.VideoFrame> reader)
        {
            var id = Guid.NewGuid();
            var ch = Channel.CreateBounded<IntercomStreamService.VideoFrame>(new BoundedChannelOptions(300)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest, // живое видео: лучше пропуск, чем лаг
            });
            _subs[id] = ch;
            reader = ch.Reader;
            return new Unsubscriber(this, id);
        }

        private sealed class Unsubscriber(Session s, Guid id) : IDisposable
        {
            public void Dispose()
            {
                if (s._subs.TryRemove(id, out _) && s._subs.IsEmpty)
                    s._owner.Release(s);
            }
        }

        public void Shutdown(string reason)
        {
            if (_completed) return;
            _completed = true;
            try { _owner._logger.LogInformation("Ch1 transcoder stopping: {Reason}", reason); } catch { }
            try { _cts.Cancel(); } catch { }
            try { _ffmpeg.Kill(); } catch { }
            _sourceSub.Dispose();
            foreach (var (_, ch) in _subs)
                ch.Writer.TryComplete();
        }

        private async Task PumpInputAsync(ChannelReader<IntercomStreamService.VideoFrame> src)
        {
            try
            {
                var stdin = _ffmpeg.StandardInput.BaseStream;
                await stdin.WriteAsync(Program.Ch1Headers, _cts.Token);
                await stdin.WriteAsync(Program.Ch1Idr, _cts.Token);
                await foreach (var frame in src.ReadAllAsync(_cts.Token))
                    await stdin.WriteAsync(frame.Payload, _cts.Token);
                await stdin.FlushAsync(_cts.Token);
            }
            catch (Exception e) when (e is OperationCanceledException or IOException) { /* штатно */ }
            catch (Exception e)
            {
                _owner._logger.LogWarning(e, "Ch1 transcoder stdin failed");
            }
        }

        /// <summary>
        /// Читает stdout FFmpeg, нарезает кадры по AUD (x264 с aud=1 ставит AUD
        /// перед каждым кадром, включая SPS/PPS/IDR в начале) и рассылает подписчикам.
        /// </summary>
        private async Task PumpOutputAsync()
        {
            const int durMs = 40; // 25 fps
            var stdout = _ffmpeg.StandardOutput.BaseStream;
            var buf = new byte[64 * 1024];
            var pending = new MemoryStream();

            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    var read = await stdout.ReadAsync(buf, _cts.Token);
                    if (read == 0) break; // FFmpeg закрыл stdout
                    pending.Write(buf, 0, read);

                    var data = pending.GetBuffer();
                    var len = (int)pending.Length;

                    // Ищем последовательные AUD; кадр = [AUD … следующий AUD).
                    int cut = -1;
                    int firstAud = FindAud(data, len, 0);
                    if (firstAud < 0) continue;

                    int cur = firstAud;
                    while (true)
                    {
                        int next = FindAud(data, len, cur + 4);
                        if (next < 0) break;
                        var frame = new byte[next - cur];
                        Array.Copy(data, cur, frame, 0, next - cur);
                        Publish(new IntercomStreamService.VideoFrame(frame, durMs));
                        cur = next;
                        cut = next;
                    }

                    if (cut > 0)
                    {
                        pending.Position = cut; // отбрасываем отправленное
                        var rest = new MemoryStream();
                        await rest.WriteAsync(data.AsMemory(cut, len - cut), _cts.Token);
                        pending = rest;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _owner._logger.LogWarning(e, "Ch1 transcoder stdout failed");
            }
            finally
            {
                _completed = true;
                foreach (var (_, ch) in _subs)
                    ch.Writer.TryComplete();
                if (!_cts.IsCancellationRequested)
                    _owner._logger.LogWarning("Ch1 transcoder stream ended");
            }
        }

        private void Publish(IntercomStreamService.VideoFrame frame)
        {
            foreach (var (_, ch) in _subs)
                ch.Writer.TryWrite(frame);
        }

        /// <summary>Поиск "00 00 01 09" (AUD, любой nal_ref_idc) от позиции from.</summary>
        private static int FindAud(byte[] data, int len, int from)
        {
            for (var i = Math.Max(0, from); i < len - 4; i++)
            {
                if (data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 && (data[i + 3] & 0x1f) == 9)
                    return i;
            }
            return -1;
        }

        public void Dispose() => Shutdown("disposal");
    }
}
