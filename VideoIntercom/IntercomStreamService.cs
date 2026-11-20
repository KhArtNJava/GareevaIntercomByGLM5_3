using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;

namespace VideoIntercom;

/// <summary>
/// Фоновый сервис: подключается к видеодомофону (MDVR-протокол) по TCP:9000,
/// отправляет бинарный handshake (toServer720.bin) и разбирает входящий поток.
///
/// Формат пакетов (установлен эмпирически по дампам):
///   "NNdcH264"          — ASCII-маркер кадра (NN = номер канала: 00, 01)
///   u32 LE              — размер кадра
///   u32 LE              — флаги/счётчик
///   u32 LE              — метка времени (мкс, монотонная)
///   u16 LE              — 0x0304
///   ... переменный заголовок ...
///   00 00 00 01 ...     — Annex-B H.264 (SPS/PPS/IDR/P-фреймы)
///
/// Канал 00 — 704×576 (sub-stream, SPS+PPS приходят с каждым IDR-кадром),
/// канал 01 — 720p @ ~20 fps (основной, заголовки шлёт только в начале сессии).
/// </summary>
public sealed class IntercomStreamService : BackgroundService
{
    public const string DefaultHost = "192.168.31.150";
    public const int DefaultPort = 9000;

    /// <summary>Бинарный запрос инициализации видеопотока (toServer720.bin).</summary>
    private static readonly byte[] Handshake = Convert.FromBase64String(
        """
        AAAAAQAAAAMDAAAAAAAAAAAAAGgAAAABAAAAEAAAAAEAAAABAAAAAQDWmwUAAAAAUDHjAgAA
        dQC41ZsFHlWzdwAAAAAE1psFAAAAAFAx4wLpVbN3BNabBVAx4wJo1nESAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
        AAAAAAAAAAAAAAAAAAA=
        """);

    private static ReadOnlySpan<byte> StartCode => [0x00, 0x00, 0x00, 0x01];
    private static ReadOnlySpan<byte> MarkerSuffix => "dcH264"u8;

    private readonly ILogger<IntercomStreamService> _logger;
    private readonly ConcurrentDictionary<Guid, Subscriber> _subscribers = new();

    /// <summary>Кадр видео с длительностью показа (мс) из таймстампа пакета.</summary>
    public sealed record VideoFrame(byte[] Payload, int DurationMs);

    private sealed record Subscriber(int Channel, Channel<VideoFrame> Queue);

    public IntercomStreamService(ILogger<IntercomStreamService> logger) => _logger = logger;

    /// <summary>Состояние подключения к домофону (для UI и /videostatus).</summary>
    public volatile string Status = "Ожидание подключения…";

    /// <summary>Сколько кадров извлечено из потока с момента запуска.</summary>
    public long FramesTotal;

    /// <summary>Время последнего полученного кадра (UTC) или null.</summary>
    public DateTime? LastFrameUtc;

    /// <summary>Подписаться на кадры канала. Возврат — IDisposable для отписки.</summary>
    public IDisposable Subscribe(int channel, out ChannelReader<VideoFrame> reader)
    {
        var queue = Channel.CreateBounded<VideoFrame>(new BoundedChannelOptions(600)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.DropOldest, // живое видео: лучше пропустить, чем копить
        });
        var sub = new Subscriber(channel, queue);
        var id = Guid.NewGuid();
        _subscribers[id] = sub;
        reader = queue.Reader;
        return new Unsubscriber(this, id);
    }

    private sealed class Unsubscriber(IntercomStreamService svc, Guid id) : IDisposable
    {
        public void Dispose() => svc._subscribers.TryRemove(id, out _);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                Status = $"Подключение к {DefaultHost}:{DefaultPort}…";
                using var client = new TcpClient();
                using var reg = stoppingToken.Register(() => client.Dispose());
                await client.ConnectAsync(DefaultHost, DefaultPort, stoppingToken);

                Status = $"Подключено к {DefaultHost}:{DefaultPort}, поток активен";
                _logger.LogInformation("Connected to intercom {Host}:{Port}", DefaultHost, DefaultPort);

                var stream = client.GetStream();
                await stream.WriteAsync(Handshake, stoppingToken);
                _logger.LogInformation("Handshake sent ({Bytes} bytes)", Handshake.Length);

                await PumpAsync(stream, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Status = $"Связь потеряна ({ex.Message}); переподключение через 3 с…";
                _logger.LogWarning(ex, "Intercom stream failed, reconnecting in 3s");
                try { await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }
        Status = "Сервис остановлен";
    }

    /// <summary>
    /// Читает TCP-поток, выделяет кадры и рассылает подписчикам.
    /// Кадр = всё от start code пакета до начала следующего маркера;
    /// публикуется, когда начнёт приходить следующий пакет (граница известна).
    /// </summary>
    private async Task PumpAsync(NetworkStream stream, CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        byte[] leftover = [];
        const int maxBuffer = 4_000_000; // защита от рассинхронизации протокола
        var lastTs = new Dictionary<int, long>();

        while (!ct.IsCancellationRequested)
        {
            var read = await stream.ReadAsync(buffer, ct);
            if (read == 0) throw new IOException("Домофон закрыл соединение");

            var data = new byte[leftover.Length + read];
            leftover.CopyTo(data, 0);
            buffer.AsSpan(0, read).CopyTo(data.AsSpan(leftover.Length));

            var parsed = ParseChunk(data, lastTs);
            leftover = parsed.Leftover;

            if (leftover.Length > maxBuffer)
            {
                _logger.LogWarning("Packet desync ({Bytes} bytes), resyncing", leftover.Length);
                leftover = [];
            }

            foreach (var f in parsed.Frames)
                Publish(f.Channel, f.Data, f.DurationMs);
        }
    }

    /// <summary>Разбор буфера на кадры + непотреблённый хвост.</summary>
    private static (List<Frame> Frames, byte[] Leftover) ParseChunk(byte[] data, Dictionary<int, long> lastTs)
    {
        var frames = new List<Frame>();
        var pos = 0;

        while (true)
        {
            // Маркер "NNdcH264" + u32 size + u32 x + u64 µs-таймстамп
            if (!TryFindMarker(data, pos, out var markerPos, out var channel))
                return (frames, data.Length > 8 ? data[^8..] : data);

            if (markerPos + 24 > data.Length)
                return (frames, data[markerPos..]); // заголовок не докачан

            var ts = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(markerPos + 16));

            var sc = IndexOfStartCode(data, markerPos + 24);
            if (sc < 0)
                return (frames, data[markerPos..]); // start code не пришёл

            if (!TryFindMarker(data, sc + 4, out var nextMarker, out _))
                return (frames, data[markerPos..]); // хвост кадра не докачан

            // Длительность кадра = разница µs-таймстампов соседних пакетов канала
            int durMs = 0;
            if (lastTs.TryGetValue(channel, out var prev) && ts > prev && ts - prev < 5_000_000)
                durMs = (int)Math.Clamp((ts - prev) / 1000, 1, 1000);
            lastTs[channel] = ts;

            frames.Add(new Frame(channel, data[sc..nextMarker], durMs));
            pos = nextMarker;
        }
    }

    private sealed record Frame(int Channel, byte[] Data, int DurationMs);

    /// <summary>Ищет "NNdcH264", где NN — две ASCII-цифры (номер канала).</summary>
    private static bool TryFindMarker(byte[] data, int from, out int markerPos, out int channel)
    {
        markerPos = -1;
        channel = -1;
        var span = data.AsSpan(from);
        var idx = span.IndexOf(MarkerSuffix);
        while (idx >= 0)
        {
            var abs = from + idx;
            if (abs >= 2)
            {
                var a = data[abs - 2];
                var b = data[abs - 1];
                if (a is >= (byte)'0' and <= (byte)'9' && b is >= (byte)'0' and <= (byte)'9')
                {
                    markerPos = abs - 2;
                    channel = (a - '0') * 10 + (b - '0');
                    return true;
                }
            }
            span = span[(idx + 1)..];
            idx = span.IndexOf(MarkerSuffix);
            from = abs + 1;
        }
        return false;
    }

    private static int IndexOfStartCode(byte[] data, int from)
    {
        var span = data.AsSpan(from);
        var idx = span.IndexOf(StartCode);
        return idx < 0 ? -1 : from + idx;
    }

    private void Publish(int channel, byte[] frame, int durationMs)
    {
        Interlocked.Increment(ref FramesTotal);
        LastFrameUtc = DateTime.UtcNow;

        foreach (var (_, sub) in _subscribers)
        {
            if (sub.Channel == channel)
                sub.Queue.Writer.TryWrite(new VideoFrame(frame, durationMs));
        }
    }
}
