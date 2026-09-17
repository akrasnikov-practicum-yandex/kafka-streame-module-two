package ru.practicum.kafka.streams;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.practicum.kafka.streams.config.AppConfig;
import ru.practicum.kafka.streams.topology.MessagingTopology;

/**
 * Точка входа. Вся логика обработки — в {@link MessagingTopology}.
 */
public final class MessagingStreamsApp {

    private static final Logger log = LoggerFactory.getLogger(MessagingStreamsApp.class);

    public static void main(String[] args) {
        AppConfig config = AppConfig.fromEnvironment();
        Topology topology = MessagingTopology.build(config);

        // Описание топологии в логе: по нему видно фактический план выполнения —
        // источники, узлы обработки, хранилища и стоки.
        log.info("Топология:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config.toStreamsProperties());
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        // Необработанное исключение в потоке обработки не должно тихо гасить
        // приложение: поток заменяется на новый, а сообщение уходит в лог.
        streams.setUncaughtExceptionHandler(throwable -> {
            log.error("Ошибка в потоке обработки, поток будет перезапущен.", throwable);
            return REPLACE_THREAD;
        });

        streams.setStateListener((newState, oldState) ->
                log.info("Состояние приложения: {} -> {}", oldState, newState));

        // docker stop посылает SIGTERM. close() досылает буферы, коммитит смещения
        // и выходит из группы — иначе партиции освободились бы только по таймауту.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Получен сигнал завершения, останавливаем приложение...");
            streams.close();
            shutdownLatch.countDown();
        }, "streams-shutdown-hook"));

        try {
            streams.start();
            log.info("Приложение запущено. Брокеры={}, application.id={}.",
                    config.bootstrapServers(), config.applicationId());

            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Ожидание завершения прервано.");
        } catch (Exception e) {
            log.error("Не удалось запустить приложение.", e);
            System.exit(1);
        }

        log.info("Приложение остановлено.");
    }
}
