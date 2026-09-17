using System.Text.Json.Serialization;

namespace Practicum.Kafka.Streams.Models;

/// <summary>
/// Команда изменения списка блокировок — запись топика blocked_users.
///
/// В топик кладутся именно команды, а не готовый список: чтобы заблокировать
/// кого-то, клиенту достаточно знать одну пару «кто кого», а не весь текущий
/// список пользователя. Из потока команд топология сворачивает актуальное
/// множество, так что состояние восстанавливается проигрыванием топика с начала.
///
/// Ключ записи Kafka — blocker_id: он же ключ таблицы блокировок, и именно
/// по нему поток сообщений совмещается с ней без repartition.
/// </summary>
public sealed record BlockCommand
{
    public const string ActionBlock = "block";

    [JsonPropertyName("blocker_id")]
    public string BlockerId { get; init; } = string.Empty;

    [JsonPropertyName("blocked_id")]
    public string BlockedId { get; init; } = string.Empty;

    [JsonPropertyName("action")]
    public string Action { get; init; } = string.Empty;

    [JsonIgnore]
    public bool IsBlock => string.Equals(Action, ActionBlock, StringComparison.OrdinalIgnoreCase);
}
