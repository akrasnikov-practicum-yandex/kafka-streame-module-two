using System.Text.Json.Serialization;

namespace Practicum.Kafka.Streams.Models;

/// <summary>
/// Сообщение чата — единый формат для топиков messages и filtered_messages.
///
/// Имена полей заданы заданием 2 (ksqlDB), и задание 1 использует ту же схему:
/// один формат на обе части работы избавляет от конвертации между ними.
/// Отправитель называется user_id, а не sender_id, именно поэтому.
///
/// Timestamp — строка ISO-8601, а не число: так сообщение читается глазами
/// в логах и в Kafka UI. Оконных агрегаций в работе нет, поэтому разбирать её
/// в метку времени Kafka незачем.
/// </summary>
public sealed record ChatMessage
{
    [JsonPropertyName("user_id")]
    public string UserId { get; init; } = string.Empty;

    [JsonPropertyName("recipient_id")]
    public string RecipientId { get; init; } = string.Empty;

    [JsonPropertyName("message")]
    public string Message { get; init; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public string Timestamp { get; init; } = string.Empty;
}
