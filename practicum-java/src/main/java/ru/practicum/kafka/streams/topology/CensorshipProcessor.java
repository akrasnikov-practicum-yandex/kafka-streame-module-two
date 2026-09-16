package ru.practicum.kafka.streams.topology;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.practicum.kafka.streams.model.ChatMessage;
import ru.practicum.kafka.streams.util.WordMasker;

/**
 * Цензура сообщений: маскирует слова из глобального справочника.
 *
 * <p>Почему Processor API, а не join в DSL: join сопоставляет одну запись потока
 * ровно с одной записью таблицы по вычисляемому ключу, а здесь на каждое сообщение
 * нужно столько обращений к справочнику, сколько в нём слов. Прямой доступ
 * к хранилищу выражает это естественно.
 *
 * <p>{@link FixedKeyProcessor} — вариант процессора, который не даёт менять ключ
 * записи. Именно поэтому топология не теряет партиционирование и Kafka Streams
 * не вставляет repartition-топик перед записью результата.
 */
public class CensorshipProcessor implements FixedKeyProcessor<String, ChatMessage, ChatMessage> {

    private static final Logger log = LoggerFactory.getLogger(CensorshipProcessor.class);

    private final String storeName;
    private FixedKeyProcessorContext<String, ChatMessage> context;
    private KeyValueStore<String, String> bannedWords;

    public CensorshipProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, ChatMessage> context) {
        this.context = context;
        // Глобальное хранилище доступно процессору на чтение автоматически:
        // подключать его через connectStateStores не нужно (и нельзя — Kafka Streams
        // ответит «StateStore ... is not added yet»).
        this.bannedWords = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, ChatMessage> record) {
        ChatMessage message = record.value();

        if (message == null || message.message() == null) {
            // Битое сообщение: Serde уже написал причину в лог. Пропускаем запись
            // дальше без изменений, чтобы не терять факт её существования.
            context.forward(record);
            return;
        }

        String censored = WordMasker.mask(message.message(), word -> bannedWords.get(word) != null);

        if (!censored.equals(message.message())) {
            log.info("Сообщение отцензурировано [{} -> {}]: {}",
                    message.userId(), message.recipientId(), censored);
        }

        context.forward(record.withValue(message.withMessage(censored)));
    }
}
