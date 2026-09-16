package ru.practicum.kafka.streams.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Конфигурация приложения из переменных окружения.
 *
 * <p>Значения по умолчанию рассчитаны на запуск без Docker (брокер на localhost),
 * docker-compose перекрывает их переменными окружения.
 */
public record AppConfig(
        String bootstrapServers,
        String applicationId,
        String stateDir,
        String messagesTopic,
        String filteredMessagesTopic,
        String blockedUsersTopic,
        String bannedWordsTopic) {

    public static AppConfig fromEnvironment() {
        return new AppConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("KAFKA_APPLICATION_ID", "practicum3-messaging-streams"),
                env("KAFKA_STATE_DIR", "/tmp/practicum3-state"),
                env("KAFKA_TOPIC_MESSAGES", "messages"),
                env("KAFKA_TOPIC_FILTERED_MESSAGES", "filtered_messages"),
                env("KAFKA_TOPIC_BLOCKED_USERS", "blocked_users"),
                env("KAFKA_TOPIC_BANNED_WORDS", "banned_words"));
    }

    /** Настройки для {@code KafkaStreams}. */
    public Properties toStreamsProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Каталог persistent state store. В контейнере примонтирован volume,
        // иначе состояние пропадало бы при пересоздании контейнера.
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        // Serde по умолчанию. Везде, где тип важен, он задан явно в топологии,
        // но Kafka Streams требует значения и для служебных операций.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Читать топики с начала: приложение должно увидеть блокировки и слова,
        // записанные до его первого запуска, иначе состояние соберётся неполным.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Брокер один — реплицировать служебные топики (changelog, repartition) не на что.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        return props;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
