package ru.practicum.kafka.streams.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import ru.practicum.kafka.streams.model.BlockCommand;
import ru.practicum.kafka.streams.model.ChatMessage;

/** Готовые Serde приложения — чтобы типы не собирались заново в каждом месте топологии. */
public final class AppSerdes {

    private AppSerdes() {
    }

    public static Serde<ChatMessage> chatMessage() {
        return new JsonSerde<>(new TypeReference<>() {
        });
    }

    public static Serde<BlockCommand> blockCommand() {
        return new JsonSerde<>(new TypeReference<>() {
        });
    }

    /**
     * Serde для значения таблицы блокировок.
     *
     * <p>Готового Serde для коллекций в Kafka Streams нет, поэтому множество
     * сериализуется как JSON-массив. Этим же Serde пользуется changelog-топик,
     * из которого состояние восстанавливается после перезапуска.
     */
    public static Serde<Set<String>> stringSet() {
        return new JsonSerde<>(new TypeReference<>() {
        });
    }
}
