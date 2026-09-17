using Microsoft.Extensions.Logging;
using Practicum.Kafka.Streams.Models;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.State;

namespace Practicum.Kafka.Streams.Topology;

/// <summary>
/// Обе стадии обработки: сначала блокировка отправителя, затем цензура слов.
/// Возвращает null, если сообщение заблокировано — топология отсеивает такие записи.
/// </summary>
public class MessageFilterTransformer : ITransformer<string, ChatMessage, string, ChatMessage?>
{
    private ITimestampedKeyValueStore<string, HashSet<string>> _blockedUsers = null!;
    private ITimestampedKeyValueStore<string, string> _bannedWords = null!;
    private ILogger _logger = null!;

    public void Init(ProcessorContext<string, ChatMessage?> context)
    {
        // Хранилища доступны по именам, заданным при объявлении таблиц в топологии.
        // Тип именно timestamped: материализованная таблица хранит рядом со значением
        // метку времени записи, поэтому обычный key-value-интерфейс тут не подходит.
        _blockedUsers = (ITimestampedKeyValueStore<string, HashSet<string>>)
            context.GetStateStore(MessagingTopology.BlockedUsersStore);
        _bannedWords = (ITimestampedKeyValueStore<string, string>)
            context.GetStateStore(MessagingTopology.BannedWordsStore);
        _logger = MessagingTopology.LoggerFactory.CreateLogger<MessageFilterTransformer>();
    }

    public Record<string, ChatMessage?> Process(Record<string, ChatMessage> record)
    {
        var message = record.Value;

        if (message is null)
        {
            // Битое сообщение: SerDes уже написал причину в лог.
            return Record<string, ChatMessage?>.Create(record.Key, null);
        }

        if (IsBlocked(message))
        {
            _logger.LogInformation("Сообщение отброшено: {Sender} заблокирован у {Recipient}",
                message.UserId, message.RecipientId);

            return Record<string, ChatMessage?>.Create(record.Key, null);
        }

        return Record<string, ChatMessage?>.Create(record.Key, Censor(message));
    }

    /// <summary>
    /// Сообщение не доходит, если ПОЛУЧАТЕЛЬ заблокировал ОТПРАВИТЕЛЯ.
    /// Ключ хранилища — тот, кто блокирует, то есть получатель этого сообщения.
    /// </summary>
    private bool IsBlocked(ChatMessage message)
    {
        var blocked = _blockedUsers.Get(message.RecipientId)?.Value;

        return blocked is not null && blocked.Contains(message.UserId);
    }

    private ChatMessage Censor(ChatMessage message)
    {
        if (message.Message is null)
        {
            return message;
        }

        // Слово считается запрещённым, если оно есть в справочнике и не удалено
        // tombstone'ом (у удалённого ключа значение внутри обёртки пустое).
        var censored = WordMasker.Mask(message.Message,
            word => _bannedWords.Get(word)?.Value is not null);

        if (censored == message.Message)
        {
            return message;
        }

        _logger.LogInformation("Сообщение отцензурировано [{From} -> {To}]: {Text}",
            message.UserId, message.RecipientId, censored);

        return message with { Message = censored! };
    }

    public void Close()
    {
    }
}
