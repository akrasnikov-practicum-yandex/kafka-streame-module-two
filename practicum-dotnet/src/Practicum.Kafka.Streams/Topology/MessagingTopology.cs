using Microsoft.Extensions.Logging;
using Practicum.Kafka.Streams.Configuration;
using Practicum.Kafka.Streams.Models;
using Practicum.Kafka.Streams.Serialization;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Practicum.Kafka.Streams.Topology;

/// <summary>
/// Топология обработки сообщений: блокировка отправителей, затем цензура слов.
/// Схема графа — в README, раздел «Архитектура».
/// </summary>
public static class MessagingTopology
{
    // Обе стадии живут в одном графе: промежуточного топика между блокировкой
    // и цензурой нет, это внутренний шаг топологии.
    //
    // banned_words добавлен сверх списка топиков задания: «динамически обновляемый
    // список слов» обязан где-то жить, а топик — единственный способ менять его
    // без перезапуска приложения.

    public const string BlockedUsersStore = "blocked-users-store";
    public const string BannedWordsStore = "banned-words-store";

    /// <summary>
    /// Фабрика логгеров для процессоров: Streamiz создаёт их сам, через конструктор
    /// без параметров, поэтому внедрить зависимость обычным способом некуда.
    /// </summary>
    public static ILoggerFactory LoggerFactory { get; set; } = Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddConsole());

    public static Streamiz.Kafka.Net.Stream.Topology Build(AppConfig config)
    {
        var builder = new StreamBuilder();

        // Обе таблицы объявляются, чтобы их хранилища попали в граф; читает их
        // трансформер напрямую, по именам хранилищ.
        BuildBlockedUsersTable(builder, config);
        BuildBannedWordsTable(builder, config);

        builder
            .Stream<string, ChatMessage, StringSerDes, ChatMessageSerDes>(config.MessagesTopic)
            // Блокировка и цензура делаются одним трансформером с прямым доступом
            // к обоим хранилищам, без DSL-join. Причина — в README, «Почему не join».
            //
            // Хранилище блокировок подключается третьим аргументом; глобальное
            // хранилище слов подключать не нужно и нельзя — оно доступно всегда.
            .TransformValues(
                TransformerBuilder
                    .New<string, ChatMessage, string, ChatMessage?>()
                    .Transformer<MessageFilterTransformer>()
                    .Build(),
                null,
                BlockedUsersStore)
            // Трансформер отдаёт null для заблокированных — такие записи отсеиваются.
            .Filter((_, message, _) => message is not null)
            .To(config.FilteredMessagesTopic, new StringSerDes(), new NullableChatMessageSerDes());

        return builder.Build();
    }

    /// <summary>
    /// Список заблокированных на каждого пользователя — persistent state store.
    ///
    /// Поток команд block/unblock сворачивается в множество: одна запись таблицы
    /// на пользователя, значение — кого он заблокировал. Задание требует «отдельный
    /// список заблокированных для каждого пользователя», и это ровно оно.
    /// </summary>
    private static IKTable<string, HashSet<string>> BuildBlockedUsersTable(StreamBuilder builder, AppConfig config)
    {
        return builder
            .Stream<string, BlockCommand, StringSerDes, BlockCommandSerDes>(config.BlockedUsersTopic)
            // Битые команды SerDes превратил в null — до аккумулятора они дойти не должны.
            .Filter((blockerId, command, _) => blockerId is not null && command is not null)
            .GroupByKey<StringSerDes, BlockCommandSerDes>()
            .Aggregate(
                () => new HashSet<string>(),
                ApplyBlockCommand,
                // RocksDb.As — хранилище на диске, тот же движок, что у Kafka Streams
                // в Java. Задание требует именно persistent state store.
                RocksDb.As<string, HashSet<string>>(BlockedUsersStore)
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new StringSetSerDes()));
    }

    /// <summary>
    /// Справочник запрещённых слов — глобальная таблица.
    ///
    /// Глобальная, а не обычная, по двум причинам. Во-первых, справочник не связан
    /// с ключом сообщения, поэтому обычная таблица требовала бы совмещения партиций,
    /// которого здесь нет. Во-вторых, каждый экземпляр приложения получает полную
    /// копию справочника — сколько бы их ни было запущено.
    ///
    /// Удаление слова приходит записью со значением null (tombstone) — таблица
    /// убирает такой ключ сама, отдельная команда «remove» не нужна.
    /// </summary>
    private static IGlobalKTable<string, string> BuildBannedWordsTable(StreamBuilder builder, AppConfig config)
    {
        return builder.GlobalTable<string, string, StringSerDes, StringSerDes>(
            config.BannedWordsTopic,
            RocksDb.As<string, string>(BannedWordsStore)
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new StringSerDes()));
    }

    /// <summary>Аккумулятор таблицы блокировок: применяет одну команду к множеству.</summary>
    private static HashSet<string> ApplyBlockCommand(string blockerId, BlockCommand command, HashSet<string> blocked)
    {
        var logger = LoggerFactory.CreateLogger(nameof(MessagingTopology));

        // Возвращается НОВОЕ множество, а не изменённое на месте: агрегат отдаёт
        // значение, которое уходит в state store и changelog, и мутация исходного
        // объекта приводит к тому, что join видит устаревшее состояние.
        var updated = new HashSet<string>(blocked);

        if (command.IsBlock)
        {
            updated.Add(command.BlockedId);
            logger.LogInformation("{Blocker} заблокировал {Blocked}", blockerId, command.BlockedId);
        }
        else
        {
            updated.Remove(command.BlockedId);
            logger.LogInformation("{Blocker} разблокировал {Blocked}", blockerId, command.BlockedId);
        }

        return updated;
    }
}
