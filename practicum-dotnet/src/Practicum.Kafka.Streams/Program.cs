using Microsoft.Extensions.Logging;
using Practicum.Kafka.Streams.Configuration;
using Practicum.Kafka.Streams.Topology;
using Serilog;
using Streamiz.Kafka.Net;

// Точка входа. Вся логика обработки — в MessagingTopology.

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Is(ParseLevel(Environment.GetEnvironmentVariable("LOG_LEVEL")))
    // Внутренняя диагностика клиента Kafka очень подробна и на обычном уровне
    // заглушает логи самого приложения.
    .MinimumLevel.Override("Streamiz", Serilog.Events.LogEventLevel.Warning)
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss}] [{Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var loggerFactory = LoggerFactory.Create(builder => builder.AddSerilog(Log.Logger));
MessagingTopology.LoggerFactory = loggerFactory;

var logger = loggerFactory.CreateLogger("MessagingStreamsApp");
var config = AppConfig.FromEnvironment();

KafkaStream? stream = null;

try
{
    var topology = MessagingTopology.Build(config);

    // Описание топологии в логе: по нему видно фактический план выполнения —
    // источники, узлы обработки, хранилища и стоки.
    logger.LogInformation("Топология:\n{Topology}", topology.Describe());

    stream = new KafkaStream(topology, config.ToStreamConfig());

    stream.StateChanged += (oldState, newState) =>
        logger.LogInformation("Состояние приложения: {Old} -> {New}", oldState, newState);

    // docker stop посылает SIGTERM. Dispose досылает буферы, коммитит смещения
    // и выходит из группы — иначе партиции освободились бы только по таймауту.
    var shutdown = new ManualResetEventSlim(false);

    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        shutdown.Set();
    };

    AppDomain.CurrentDomain.ProcessExit += (_, _) => shutdown.Set();

    await stream.StartAsync();
    logger.LogInformation("Приложение запущено. Брокеры={Brokers}, application.id={ApplicationId}.",
        config.BootstrapServers, config.ApplicationId);

    shutdown.Wait();
    logger.LogInformation("Получен сигнал завершения, останавливаем приложение...");

    return 0;
}
catch (Exception e)
{
    logger.LogCritical(e, "Непредвиденная ошибка, приложение останавливается.");
    return 1;
}
finally
{
    stream?.Dispose();
    logger.LogInformation("Приложение остановлено.");

    // Serilog буферизует запись, поэтому перед выходом логи нужно дослать.
    await Log.CloseAndFlushAsync();
}

static Serilog.Events.LogEventLevel ParseLevel(string? value) =>
    Enum.TryParse<Serilog.Events.LogEventLevel>(value, ignoreCase: true, out var level)
        ? level
        : Serilog.Events.LogEventLevel.Information;
