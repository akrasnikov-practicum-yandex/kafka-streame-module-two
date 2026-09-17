using System.Text.RegularExpressions;

namespace Practicum.Kafka.Streams;

/// <summary>
/// Маскирование запрещённых слов в тексте.
///
/// Вынесено из процессора отдельной чистой функцией: так логику можно проверить
/// обычным тестом, без топологии и брокера.
/// </summary>
public static partial class WordMasker
{
    private const char MaskChar = '*';

    /// <summary>
    /// Словом считается последовательность букв и цифр любого алфавита.
    /// Явный класс символов вместо \b: в .NET \w по умолчанию включает Unicode,
    /// но опора на это неочевидна читателю, а здесь важно, что кириллица
    /// обрабатывается так же, как латиница.
    /// </summary>
    [GeneratedRegex(@"[\p{L}\p{N}]+")]
    private static partial Regex WordPattern();

    /// <summary>
    /// Заменяет каждое слово, для которого isBanned вернул true, на звёздочки
    /// той же длины. Пунктуация и пробелы сохраняются, длина текста не меняется.
    /// </summary>
    /// <param name="text">исходный текст сообщения</param>
    /// <param name="isBanned">проверка слова по справочнику; слово приходит в нижнем регистре</param>
    public static string? Mask(string? text, Func<string, bool> isBanned)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        return WordPattern().Replace(text, match =>
        {
            // Справочник хранится в нижнем регистре, поэтому сравнение
            // регистронезависимо: «Дурак» и «дурак» — одно и то же слово.
            return isBanned(match.Value.ToLowerInvariant())
                ? new string(MaskChar, match.Length)
                : match.Value;
        });
    }
}
