using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Practicum.Kafka.Streams.Configuration;

/// <summary>
/// Конфигурация приложения из переменных окружения.
///
/// Значения по умолчанию рассчитаны на запуск без Docker (брокер на localhost),
/// docker-compose перекрывает их переменными окружения.
/// </summary>
public sealed record AppConfig
{
    public required string BootstrapServers { get; init; }

    public required string ApplicationId { get; init; }

    public required string StateDir { get; init; }

    public required string MessagesTopic { get; init; }

    public required string FilteredMessagesTopic { get; init; }

    public required string BlockedUsersTopic { get; init; }

    public required string BannedWordsTopic { get; init; }

    public static AppConfig FromEnvironment() => new()
    {
        BootstrapServers = Env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        ApplicationId = Env("KAFKA_APPLICATION_ID", "practicum3-messaging-streams"),
        StateDir = Env("KAFKA_STATE_DIR", Path.Combine(Path.GetTempPath(), "practicum3-state")),
        MessagesTopic = Env("KAFKA_TOPIC_MESSAGES", "messages"),
        FilteredMessagesTopic = Env("KAFKA_TOPIC_FILTERED_MESSAGES", "filtered_messages"),
        BlockedUsersTopic = Env("KAFKA_TOPIC_BLOCKED_USERS", "blocked_users"),
        BannedWordsTopic = Env("KAFKA_TOPIC_BANNED_WORDS", "banned_words")
    };

    /// <summary>Настройки для KafkaStream.</summary>
    public StreamConfig ToStreamConfig()
    {
        return new StreamConfig
        {
            ApplicationId = ApplicationId,
            BootstrapServers = BootstrapServers,

            // Каталог persistent state store. В контейнере примонтирован volume,
            // иначе состояние пропадало бы при пересоздании контейнера.
            StateDir = StateDir,

            // Читать топики с начала: приложение должно увидеть блокировки и слова,
            // записанные до его первого запуска, иначе состояние соберётся неполным.
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,

            // SerDes по умолчанию. Везде, где тип важен, он задан явно в топологии,
            // но внутренние операции (в том числе чтение state store при join)
            // опираются на значения по умолчанию.
            DefaultKeySerDes = new StringSerDes(),
            DefaultValueSerDes = new StringSerDes(),

            // Брокер один — реплицировать служебные топики (changelog) не на что.
            ReplicationFactor = 1
        };
    }

    private static string Env(string name, string defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
    }
}
