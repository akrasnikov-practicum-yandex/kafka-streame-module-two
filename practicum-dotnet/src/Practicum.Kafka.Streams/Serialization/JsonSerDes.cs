using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Practicum.Kafka.Streams.Serialization;

/// <summary>
/// JSON-сериализация для любого типа.
///
/// Kafka хранит только байты, поэтому объект нужно превратить в JSON перед записью
/// и восстановить после чтения.
///
/// Битое сообщение не должно ронять приложение: десериализатор пишет ошибку
/// в консоль и возвращает default, а топология такие записи отсеивает.
/// </summary>
public class JsonSerDes<T> : AbstractSerDes<T>
{
    private static readonly JsonSerializerOptions Options = new()
    {
        // Неизвестное поле во входящем JSON — не повод падать: схема сообщения
        // может обрасти полями раньше, чем обновится это приложение.
        // В System.Text.Json это поведение по умолчанию, здесь оно зафиксировано явно.
        PropertyNameCaseInsensitive = true,

        // По умолчанию System.Text.Json экранирует всё, кроме базовой латиницы,
        // и кириллица уходит в топик последовательностями вида п — нечитаемыми
        // ни в Kafka UI, ни в выводе kafka-console-consumer. Разрешаем кириллицу
        // как есть; экранирование остаётся для символов, опасных в HTML.
        Encoder = JavaScriptEncoder.Create(UnicodeRanges.BasicLatin, UnicodeRanges.Cyrillic)
    };

    public override byte[] Serialize(T data, SerializationContext context)
    {
        // null — это tombstone, он должен остаться tombstone'ом.
        if (data is null)
        {
            return null!;
        }

        return JsonSerializer.SerializeToUtf8Bytes(data, Options);
    }

    public override T Deserialize(byte[] data, SerializationContext context)
    {
        if (data is null || data.Length == 0)
        {
            return default!;
        }

        try
        {
            return JsonSerializer.Deserialize<T>(data, Options)!;
        }
        catch (JsonException e)
        {
            // Требование задания: записать ошибку в лог и продолжить работу.
            Console.Error.WriteLine($"[ERR] Ошибка десериализации из топика {context.Topic}: {e.Message}");
            return default!;
        }
    }
}
