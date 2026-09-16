package ru.practicum.kafka.streams.topology;

import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.practicum.kafka.streams.config.AppConfig;
import ru.practicum.kafka.streams.model.BlockCommand;
import ru.practicum.kafka.streams.model.ChatMessage;
import ru.practicum.kafka.streams.serde.AppSerdes;

/**
 * Топология обработки сообщений: блокировка отправителей, затем цензура слов.
 *
 * <pre>
 *   blocked_users ──▶ aggregate ──▶ [blocked-users-store] ─┐
 *                                                          │ leftJoin
 *   messages ─────────────────────────────────────────────▶├──▶ filter ──▶ processValues ──▶ filtered_messages
 *                                                          │                    │
 *   banned_words ──▶ addGlobalStore ──▶ [banned-words-store]────────────────────┘
 * </pre>
 *
 * <p>Обе стадии — один граф в одном процессе: промежуточного топика между блокировкой
 * и цензурой нет, это внутренний шаг топологии. Топики по заданию — только
 * {@code messages}, {@code filtered_messages} и {@code blocked_users}; {@code banned_words}
 * добавлен сверх списка, потому что «динамически обновляемый список слов» обязан где-то
 * жить, а топик — единственный способ менять его без перезапуска приложения.
 */
public final class MessagingTopology {

    private static final Logger log = LoggerFactory.getLogger(MessagingTopology.class);

    public static final String BLOCKED_USERS_STORE = "blocked-users-store";
    public static final String BANNED_WORDS_STORE = "banned-words-store";

    private MessagingTopology() {
    }

    public static Topology build(AppConfig config) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Set<String>> blockedUsers = buildBlockedUsersTable(builder, config);
        addBannedWordsStore(builder, config);

        KStream<String, ChatMessage> messages = builder.stream(
                config.messagesTopic(),
                Consumed.with(Serdes.String(), AppSerdes.chatMessage()));

        messages
                // Сообщение отбрасывается, если ПОЛУЧАТЕЛЬ заблокировал ОТПРАВИТЕЛЯ.
                // Ключ сообщения — recipient_id, ключ таблицы — blocker_id, то есть
                // один и тот же пользователь: входы co-partitioned, и Kafka Streams
                // соединяет их локально, без repartition-топика.
                //
                // leftJoin + null + filter — стандартный способ отсеять записи по
                // содержимому таблицы: отдельной операции «отбросить, если нашлось»
                // в DSL нет, а обычный join просто пропустил бы сообщения тех
                // пользователей, у которых список блокировок пуст.
                .leftJoin(blockedUsers, MessagingTopology::dropIfBlocked)
                .filter((recipientId, message) -> message != null)
                // Цензура: FixedKeyProcessor не может менять ключ, поэтому
                // партиционирование результата совпадает с входным.
                .processValues((org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier<String, ChatMessage, ChatMessage>)
                        () -> new CensorshipProcessor(BANNED_WORDS_STORE))
                .to(config.filteredMessagesTopic(),
                        Produced.with(Serdes.String(), AppSerdes.chatMessage()));

        return builder.build();
    }

    /**
     * Список заблокированных на каждого пользователя — persistent state store.
     *
     * <p>Поток команд block/unblock сворачивается в множество: одна запись таблицы
     * на пользователя, значение — кого он заблокировал. Задание требует «отдельный
     * список заблокированных для каждого пользователя», и это ровно оно.
     */
    private static KTable<String, Set<String>> buildBlockedUsersTable(StreamsBuilder builder, AppConfig config) {
        return builder
                .stream(config.blockedUsersTopic(),
                        Consumed.with(Serdes.String(), AppSerdes.blockCommand()))
                // Битые команды Serde превратил в null — до аккумулятора они дойти не должны.
                .filter((blockerId, command) -> blockerId != null && command != null)
                .groupByKey(Grouped.with(Serdes.String(), AppSerdes.blockCommand()))
                .aggregate(
                        HashSet::new,
                        MessagingTopology::applyBlockCommand,
                        // persistentKeyValueStore указан явно, хотя Materialized.as(String)
                        // и так даёт хранилище на диске: задание требует именно
                        // persistent state store, и это должно быть видно в коде.
                        Materialized.<String, Set<String>>as(
                                        Stores.persistentKeyValueStore(BLOCKED_USERS_STORE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(AppSerdes.stringSet()));
    }

    /**
     * Справочник запрещённых слов — глобальное хранилище.
     *
     * <p>Глобальное, а не обычная таблица, по двум причинам. Во-первых, справочник
     * не связан с ключом сообщения, поэтому обычная KTable требовала бы совмещения
     * партиций, которого здесь нет. Во-вторых, каждый экземпляр приложения получает
     * полную копию справочника — сколько бы их ни было запущено.
     *
     * <p>{@code addGlobalStore}, а не {@code globalTable}: цензору нужен прямой доступ
     * к хранилищу для множества обращений на одно сообщение, а не DSL-join.
     */
    private static void addBannedWordsStore(StreamsBuilder builder, AppConfig config) {
        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BANNED_WORDS_STORE),
                Serdes.String(),
                Serdes.String());

        ProcessorSupplier<String, String, Void, Void> updater = () -> new BannedWordsUpdater(BANNED_WORDS_STORE);

        builder.addGlobalStore(
                storeBuilder,
                config.bannedWordsTopic(),
                Consumed.with(Serdes.String(), Serdes.String()),
                updater);
    }

    /** Аккумулятор таблицы блокировок: применяет одну команду к множеству. */
    private static Set<String> applyBlockCommand(String blockerId, BlockCommand command, Set<String> blocked) {
        if (command.isBlock()) {
            blocked.add(command.blockedId());
            log.info("{} заблокировал {}", blockerId, command.blockedId());
        } else {
            blocked.remove(command.blockedId());
            log.info("{} разблокировал {}", blockerId, command.blockedId());
        }

        return blocked;
    }

    /** Возвращает null, если отправитель в списке заблокированных получателя. */
    private static ChatMessage dropIfBlocked(ChatMessage message, Set<String> blockedByRecipient) {
        if (message == null) {
            return null;
        }

        if (blockedByRecipient != null && blockedByRecipient.contains(message.userId())) {
            log.info("Сообщение отброшено: {} заблокирован у {}", message.userId(), message.recipientId());
            return null;
        }

        return message;
    }
}
