package ru.practicum.kafka.streams.topology;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Поддерживает глобальный справочник запрещённых слов в актуальном состоянии.
 *
 * <p>{@code addGlobalStore} требует собственный процессор записи — в отличие от
 * {@code globalTable}, который наполняет хранилище сам. Здесь это оправдано:
 * глобальное хранилище нужно нам для прямых обращений из цензора, а не для join,
 * а заодно появляется место, где нормализуется регистр слов.
 *
 * <p>Динамическое обновление списка (требование задания) получается само собой:
 * запись в топик сразу меняет справочник во всех экземплярах приложения,
 * перезапуск не нужен.
 */
public class BannedWordsUpdater implements Processor<String, String, Void, Void> {

    private static final Logger log = LoggerFactory.getLogger(BannedWordsUpdater.class);

    private final String storeName;
    private KeyValueStore<String, String> store;

    public BannedWordsUpdater(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, String> record) {
        if (record.key() == null) {
            log.warn("Запись справочника без ключа пропущена: ключ — это само слово.");
            return;
        }

        // Слова хранятся в нижнем регистре: цензор ищет по нормализованной форме,
        // поэтому «Дурак» из топика и «дурак» в тексте — одно и то же слово.
        String word = record.key().toLowerCase();

        if (record.value() == null) {
            // Tombstone: значение null в compacted-топике означает удаление ключа.
            // Так слово снимается со списка — без отдельной команды «remove».
            store.delete(word);
            log.info("Слово удалено из справочника: {}", word);
            return;
        }

        store.put(word, record.value());
        log.info("Слово добавлено в справочник: {}", word);
    }
}
