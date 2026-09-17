package ru.practicum.kafka.streams.util;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Маскирование запрещённых слов в тексте.
 *
 * <p>Вынесено из процессора отдельной чистой функцией: так логику можно проверить
 * обычным юнит-тестом, без топологии и брокера.
 */
public final class WordMasker {

    /** Чем заменяется запрещённое слово. */
    private static final char MASK_CHAR = '*';

    /**
     * Границы слова заданы явным классом символов, а не {@code \b}: в регулярных
     * выражениях Java по умолчанию {@code \w} — это только латиница, цифры
     * и подчёркивание, поэтому на кириллице границы срабатывали бы неверно.
     * Здесь словом считается последовательность букв и цифр любого алфавита.
     */
    private static final Pattern WORD_PATTERN = Pattern.compile("[\\p{L}\\p{N}]+", Pattern.UNICODE_CHARACTER_CLASS);

    private WordMasker() {
    }

    /**
     * Заменяет каждое слово, для которого {@code isBanned} вернул true, на звёздочки
     * той же длины. Пунктуация и пробелы сохраняются, длина текста не меняется.
     *
     * @param text     исходный текст сообщения
     * @param isBanned проверка слова по справочнику; слово приходит в нижнем регистре
     * @return текст с замаскированными словами
     */
    public static String mask(String text, Predicate<String> isBanned) {
        if (text == null || text.isEmpty()) {
            return text;
        }

        Matcher matcher = WORD_PATTERN.matcher(text);
        StringBuilder result = new StringBuilder(text.length());

        while (matcher.find()) {
            // Справочник хранится в нижнем регистре, поэтому сравнение
            // регистронезависимо: «Дурак» и «дурак» — одно и то же слово.
            String word = matcher.group();

            if (isBanned.test(word.toLowerCase())) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(maskOf(word.length())));
            }
        }

        matcher.appendTail(result);
        return result.toString();
    }

    private static String maskOf(int length) {
        return String.valueOf(MASK_CHAR).repeat(length);
    }
}
