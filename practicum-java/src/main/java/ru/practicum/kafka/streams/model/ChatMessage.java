package ru.practicum.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Сообщение чата — единый формат для топиков messages и filtered_messages.
 *
 * <p>Имена полей заданы заданием 2 (ksqlDB), и задание 1 использует ту же схему:
 * один формат на обе части работы избавляет от конвертации между ними.
 * Отправитель называется {@code user_id}, а не {@code sender_id}, именно поэтому.
 *
 * <p>{@code timestamp} — строка ISO-8601, а не число: так сообщение читается глазами
 * в логах и в Kafka UI. Оконных агрегаций в работе нет, поэтому разбирать её
 * в метку времени Kafka незачем.
 *
 * <p>Record, а не класс: значение неизменяемо, а Jackson собирает его через
 * канонический конструктор.
 */
public record ChatMessage(
        @JsonProperty("user_id") String userId,
        @JsonProperty("recipient_id") String recipientId,
        @JsonProperty("message") String message,
        @JsonProperty("timestamp") String timestamp) {

    /** Копия сообщения с заменённым текстом — результат работы цензора. */
    public ChatMessage withMessage(String newMessage) {
        return new ChatMessage(userId, recipientId, newMessage, timestamp);
    }
}
