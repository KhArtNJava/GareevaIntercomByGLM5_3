using System.Buffers.Binary;
using System.Threading.Channels;
using VideoIntercom;
using VideoIntercom.Components;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddSingleton<IntercomStreamService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<IntercomStreamService>());

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseStatusCodePagesWithReExecute("/not-found");
app.UseHttpsRedirection();
app.UseAntiforgery();

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

// Живой H.264-поток. Протокол кадрирования: [4Б duration мс LE][4Б длина кадра LE][кадр Annex-B].
// Клиент (jmuxer) использует duration для честного тайминга MSE.
app.MapGet("/videofeed/{channel:int}", async (int channel, HttpContext ctx, IntercomStreamService intercom) =>
{
    if (channel is < 0 or > 1) return Results.NotFound(new { error = "channel must be 0 or 1" });

    ctx.Response.Headers.ContentType = "application/octet-stream";
    ctx.Response.Headers.CacheControl = "no-store";
    ctx.Response.Headers.TryAdd("X-Accel-Buffering", "no");

    var unsubscribe = intercom.Subscribe(channel, out var reader);
    var hdr = new byte[8];
    try
    {
        await ctx.Response.StartAsync(ctx.RequestAborted);
        await foreach (var frame in reader.ReadAllAsync(ctx.RequestAborted))
        {
            BinaryPrimitives.WriteInt32LittleEndian(hdr, frame.DurationMs);
            BinaryPrimitives.WriteInt32LittleEndian(hdr.AsSpan(4), frame.Payload.Length);
            await ctx.Response.Body.WriteAsync(hdr, ctx.RequestAborted);
            await ctx.Response.Body.WriteAsync(frame.Payload, ctx.RequestAborted);
        }
    }
    catch (OperationCanceledException) { /* клиент ушёл — нормальный сценарий */ }
    finally
    {
        unsubscribe.Dispose();
    }
    return Results.Empty;
})
.WithSummary("Живой H.264 поток от видеодомофона");

// Статус источника для индикатора на странице
app.MapGet("/videostatus", (IntercomStreamService intercom) => Results.Json(new
{
    status = intercom.Status,
    frames = intercom.FramesTotal,
    lastFrameUtc = intercom.LastFrameUtc,
}));

app.Run();
