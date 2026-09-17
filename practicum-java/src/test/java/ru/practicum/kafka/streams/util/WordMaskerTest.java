package ru.practicum.kafka.streams.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WordMaskerTest {

    private static final Set<String> BANNED = Set.of("дурак", "spam");

    @Test
    @DisplayName("Запрещённое слово заменяется звёздочками той же длины")
    void masksBannedWord() {
        String result = WordMasker.mask("ты дурак, шучу", BANNED::contains);

        assertEquals("ты *****, шучу", result);
    }

    @Test
    @DisplayName("Регистр не важен: справочник хранится в нижнем регистре")
    void masksIgnoringCase() {
        String result = WordMasker.mask("Дурак и SPAM", BANNED::contains);

        assertEquals("***** и ****", result);
    }

    @Test
    @DisplayName("Текст без запрещённых слов остаётся неизменным")
    void keepsCleanTextAsIs() {
        String text = "привет, как дела?";

        assertEquals(text, WordMasker.mask(text, BANNED::contains));
    }

    @Test
    @DisplayName("Часть слова не маскируется: граница слова учитывается")
    void doesNotMaskSubstring() {
        // «дурака» — другое слово, а не вхождение «дурак»: маскируется слово целиком
        // либо не маскируется вовсе.
        String result = WordMasker.mask("дурака нет", BANNED::contains);

        assertEquals("дурака нет", result);
    }

    @Test
    @DisplayName("Пустой текст и null не ломают маскирование")
    void handlesEmptyInput() {
        assertEquals("", WordMasker.mask("", BANNED::contains));
        assertEquals(null, WordMasker.mask(null, BANNED::contains));
    }
}
