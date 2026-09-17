using Practicum.Kafka.Streams;
using Xunit;

namespace Practicum.Kafka.Streams.Tests;

public class WordMaskerTests
{
    private static readonly HashSet<string> Banned = ["дурак", "spam"];

    private static bool IsBanned(string word) => Banned.Contains(word);

    [Fact(DisplayName = "Запрещённое слово заменяется звёздочками той же длины")]
    public void MasksBannedWord()
    {
        Assert.Equal("ты *****, шучу", WordMasker.Mask("ты дурак, шучу", IsBanned));
    }

    [Fact(DisplayName = "Регистр не важен: справочник хранится в нижнем регистре")]
    public void MasksIgnoringCase()
    {
        Assert.Equal("***** и ****", WordMasker.Mask("Дурак и SPAM", IsBanned));
    }

    [Fact(DisplayName = "Текст без запрещённых слов остаётся неизменным")]
    public void KeepsCleanTextAsIs()
    {
        const string text = "привет, как дела?";

        Assert.Equal(text, WordMasker.Mask(text, IsBanned));
    }

    [Fact(DisplayName = "Часть слова не маскируется: граница слова учитывается")]
    public void DoesNotMaskSubstring()
    {
        // «дурака» — другое слово, а не вхождение «дурак»: маскируется слово целиком
        // либо не маскируется вовсе.
        Assert.Equal("дурака нет", WordMasker.Mask("дурака нет", IsBanned));
    }

    [Fact(DisplayName = "Пустой текст и null не ломают маскирование")]
    public void HandlesEmptyInput()
    {
        Assert.Equal("", WordMasker.Mask("", IsBanned));
        Assert.Null(WordMasker.Mask(null, IsBanned));
    }
}
