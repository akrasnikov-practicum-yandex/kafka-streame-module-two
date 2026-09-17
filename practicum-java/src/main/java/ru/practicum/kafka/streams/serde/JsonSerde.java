package ru.practicum.kafka.streams.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON-сериализация поверх Jackson для любого типа.
 *
 * <p>Kafka хранит только байты, поэтому объект нужно превратить в JSON перед записью
 * и восстановить после чтения. Тип передаётся через {@link TypeReference}, а не
 * {@code Class}: иначе не выразить дженерики вроде {@code Set<String>} — информация
 * о параметре типа стирается при компиляции.
 *
 * <p>Битое сообщение не должно ронять приложение: десериализатор пишет ошибку в лог
 * и возвращает {@code null}, а топология такие записи отсеивает.
 */
public class JsonSerde<T> implements Serde<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonSerde.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            // Неизвестное поле во входящем JSON — не повод падать: схема сообщения
            // может обрасти полями раньше, чем обновится это приложение.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final TypeReference<T> typeReference;

    public JsonSerde(TypeReference<T> typeReference) {
        this.typeReference = typeReference;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                // null — это tombstone, он должен остаться tombstone'ом,
                // а не превратиться в четыре байта строки "null".
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                log.error("Ошибка сериализации для топика {}: {}", topic, e.getMessage());
                return null;
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, typeReference);
            } catch (Exception e) {
                // Требование задания: записать ошибку в лог и продолжить работу.
                log.error("Ошибка десериализации из топика {}: {}", topic, e.getMessage());
                return null;
            }
        };
    }
}
