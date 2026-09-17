using Practicum.Kafka.Streams.Models;

namespace Practicum.Kafka.Streams.Serialization;

/// <summary>
/// Готовые SerDes приложения.
///
/// Streamiz принимает тип SerDes параметром-дженериком (например,
/// Stream&lt;string, ChatMessage, StringSerDes, ChatMessageSerDes&gt;), поэтому
/// каждому типу нужен именованный класс — записать JsonSerDes&lt;ChatMessage&gt;
/// прямо в параметре типа нельзя.
/// </summary>
public sealed class ChatMessageSerDes : JsonSerDes<ChatMessage>;

/// <summary>
/// Тот же формат, но для nullable-типа: трансформер отдаёт null для заблокированных
/// сообщений, и до записи в топик они отсеиваются фильтром.
/// </summary>
public sealed class NullableChatMessageSerDes : JsonSerDes<ChatMessage?>;

public sealed class BlockCommandSerDes : JsonSerDes<BlockCommand>;

/// <summary>
/// SerDes значения таблицы блокировок: множество сериализуется как JSON-массив.
/// Этим же SerDes пользуется changelog-топик, из которого состояние
/// восстанавливается после перезапуска.
/// </summary>
public sealed class StringSetSerDes : JsonSerDes<HashSet<string>>;
