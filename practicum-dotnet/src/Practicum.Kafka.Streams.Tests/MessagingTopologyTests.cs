using Practicum.Kafka.Streams.Configuration;
using Practicum.Kafka.Streams.Models;
using Practicum.Kafka.Streams.Serialization;
using Practicum.Kafka.Streams.Topology;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Xunit;

namespace Practicum.Kafka.Streams.Tests;

/// <summary>
/// Прогон топологии без брокера: TopologyTestDriver выполняет тот же граф,
/// что и боевое приложение, но синхронно и в памяти.
/// </summary>
public class MessagingTopologyTests : IDisposable
{
    private readonly string _stateDir = Path.Combine(Path.GetTempPath(), $"practicum3-test-{Guid.NewGuid():N}");
    private readonly TopologyTestDriver _driver;
    private readonly TestInputTopic<string, ChatMessage> _messages;
    private readonly TestInputTopic<string, BlockCommand> _blockedUsers;
    private readonly TestInputTopic<string, string> _bannedWords;
    private readonly TestOutputTopic<string, ChatMessage> _filteredMessages;

    public MessagingTopologyTests()
    {
        var config = new AppConfig
        {
            BootstrapServers = "dummy:9092",
            ApplicationId = $"test-{Guid.NewGuid():N}",
            StateDir = _stateDir,
            MessagesTopic = "messages",
            FilteredMessagesTopic = "filtered_messages",
            BlockedUsersTopic = "blocked_users",
            BannedWordsTopic = "banned_words"
        };

        _driver = new TopologyTestDriver(MessagingTopology.Build(config), config.ToStreamConfig());

        _messages = _driver.CreateInputTopic<string, ChatMessage, StringSerDes, ChatMessageSerDes>("messages");
        _blockedUsers = _driver.CreateInputTopic<string, BlockCommand, StringSerDes, BlockCommandSerDes>("blocked_users");
        _bannedWords = _driver.CreateInputTopic<string, string, StringSerDes, StringSerDes>("banned_words");
        _filteredMessages = _driver.CreateOutputTopic<string, ChatMessage, StringSerDes, ChatMessageSerDes>("filtered_messages");
    }

    [Fact(DisplayName = "Сообщение от заблокированного отправителя не доходит до получателя")]
    public void DropsMessageFromBlockedSender()
    {
        // bob блокирует eve. Ключ команды — тот, кто блокирует.
        _blockedUsers.PipeInput("bob", Block("bob", "eve"));

        // Ключ сообщения — получатель: так поток совмещается с таблицей блокировок.
        _messages.PipeInput("bob", Message("eve", "bob", "это сообщение не дойдёт"));
        _messages.PipeInput("bob", Message("alice", "bob", "привет, как дела?"));

        var result = _filteredMessages.ReadValueList().ToList();

        Assert.Single(result);
        Assert.Equal("alice", result[0].UserId);
    }

    [Fact(DisplayName = "Разблокировка возвращает доставку сообщений")]
    public void DeliversAfterUnblock()
    {
        _blockedUsers.PipeInput("bob", Block("bob", "eve"));
        _messages.PipeInput("bob", Message("eve", "bob", "первое"));

        _blockedUsers.PipeInput("bob", Unblock("bob", "eve"));
        _messages.PipeInput("bob", Message("eve", "bob", "второе"));

        var result = _filteredMessages.ReadValueList().ToList();

        Assert.Single(result);
        Assert.Equal("второе", result[0].Message);
    }

    [Fact(DisplayName = "Запрещённое слово маскируется в тексте сообщения")]
    public void MasksBannedWord()
    {
        _bannedWords.PipeInput("дурак", "1");

        _messages.PipeInput("carol", Message("alice", "carol", "ты дурак, шучу :)"));

        var result = _filteredMessages.ReadKeyValue();

        Assert.Equal("ты *****, шучу :)", result.Message.Value.Message);
    }

    [Fact(DisplayName = "Удаление слова из справочника отменяет цензуру без перезапуска")]
    public void StopsMaskingAfterTombstone()
    {
        _bannedWords.PipeInput("дурак", "1");
        _messages.PipeInput("carol", Message("alice", "carol", "дурак"));

        // Значение null — tombstone: слово снимается со списка.
        _bannedWords.PipeInput("дурак", null!);
        _messages.PipeInput("carol", Message("alice", "carol", "дурак"));

        var result = _filteredMessages.ReadValueList().ToList();

        Assert.Equal("*****", result[0].Message);
        Assert.Equal("дурак", result[1].Message);
    }

    [Fact(DisplayName = "Блокировка и цензура применяются вместе, в одном проходе")]
    public void AppliesBothStages()
    {
        _blockedUsers.PipeInput("bob", Block("bob", "eve"));
        _bannedWords.PipeInput("дурак", "1");

        _messages.PipeInput("bob", Message("eve", "bob", "дурак"));     // отбрасывается
        _messages.PipeInput("bob", Message("alice", "bob", "дурак"));   // маскируется

        var result = _filteredMessages.ReadValueList().ToList();

        Assert.Single(result);
        Assert.Equal("alice", result[0].UserId);
        Assert.Equal("*****", result[0].Message);
    }

    [Fact(DisplayName = "Список блокировок лежит в state store и переживает обработку")]
    public void KeepsBlockedUsersInStateStore()
    {
        _blockedUsers.PipeInput("bob", Block("bob", "eve"));
        _blockedUsers.PipeInput("bob", Block("bob", "mallory"));

        var store = _driver.GetKeyValueStore<string, HashSet<string>>(MessagingTopology.BlockedUsersStore);

        Assert.Equal(["eve", "mallory"], store.Get("bob").OrderBy(x => x));
    }

    public void Dispose()
    {
        _driver.Dispose();

        try
        {
            Directory.Delete(_stateDir, recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
            // Каталог мог не создаться — нечего чистить.
        }

        GC.SuppressFinalize(this);
    }

    private static ChatMessage Message(string from, string to, string text) => new()
    {
        UserId = from,
        RecipientId = to,
        Message = text,
        Timestamp = "2026-09-16T10:00:00Z"
    };

    private static BlockCommand Block(string blocker, string blocked) => new()
    {
        BlockerId = blocker,
        BlockedId = blocked,
        Action = "block"
    };

    private static BlockCommand Unblock(string blocker, string blocked) => new()
    {
        BlockerId = blocker,
        BlockedId = blocked,
        Action = "unblock"
    };
}
