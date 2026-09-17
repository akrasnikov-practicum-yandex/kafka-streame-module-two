package ru.practicum.kafka.streams.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.practicum.kafka.streams.config.AppConfig;
import ru.practicum.kafka.streams.model.BlockCommand;
import ru.practicum.kafka.streams.model.ChatMessage;
import ru.practicum.kafka.streams.serde.AppSerdes;

/**
 * Прогон топологии без брокера: TopologyTestDriver выполняет тот же граф,
 * что и боевое приложение, но синхронно и в памяти.
 */
class MessagingTopologyTest {

    private static final AppConfig CONFIG = new AppConfig(
            "dummy:9092", "test-app", "unused",
            "messages", "filtered_messages", "blocked_users", "banned_words");

    private TopologyTestDriver driver;
    private TestInputTopic<String, ChatMessage> messages;
    private TestInputTopic<String, BlockCommand> blockedUsers;
    private TestInputTopic<String, String> bannedWords;
    private TestOutputTopic<String, ChatMessage> filteredMessages;
    private Path stateDir;

    @BeforeEach
    void setUp() throws Exception {
        stateDir = Files.createTempDirectory("practicum3-test-state");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        driver = new TopologyTestDriver(MessagingTopology.build(CONFIG), props);

        messages = driver.createInputTopic("messages",
                Serdes.String().serializer(), AppSerdes.chatMessage().serializer());
        blockedUsers = driver.createInputTopic("blocked_users",
                Serdes.String().serializer(), AppSerdes.blockCommand().serializer());
        bannedWords = driver.createInputTopic("banned_words",
                Serdes.String().serializer(), Serdes.String().serializer());
        filteredMessages = driver.createOutputTopic("filtered_messages",
                Serdes.String().deserializer(), AppSerdes.chatMessage().deserializer());
    }

    @AfterEach
    void tearDown() throws Exception {
        driver.close();

        try (var paths = Files.walk(stateDir)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                    // Временный каталог теста — неудача удаления не должна ронять сборку.
                }
            });
        }
    }

    @Test
    @DisplayName("Сообщение от заблокированного отправителя не доходит до получателя")
    void dropsMessageFromBlockedSender() {
        // bob блокирует eve. Ключ команды — тот, кто блокирует.
        blockedUsers.pipeInput("bob", new BlockCommand("bob", "eve", "block"));

        // Ключ сообщения — получатель: именно так поток совмещается с таблицей блокировок.
        messages.pipeInput("bob", message("eve", "bob", "это сообщение не дойдёт"));
        messages.pipeInput("bob", message("alice", "bob", "привет, как дела?"));

        List<ChatMessage> result = filteredMessages.readValuesToList();

        assertEquals(1, result.size(), "до получателя должно дойти только одно сообщение");
        assertEquals("alice", result.get(0).userId());
    }

    @Test
    @DisplayName("Разблокировка возвращает доставку сообщений")
    void deliversAfterUnblock() {
        blockedUsers.pipeInput("bob", new BlockCommand("bob", "eve", "block"));
        messages.pipeInput("bob", message("eve", "bob", "первое"));

        blockedUsers.pipeInput("bob", new BlockCommand("bob", "eve", "unblock"));
        messages.pipeInput("bob", message("eve", "bob", "второе"));

        List<ChatMessage> result = filteredMessages.readValuesToList();

        assertEquals(1, result.size());
        assertEquals("второе", result.get(0).message());
    }

    @Test
    @DisplayName("Запрещённое слово маскируется в тексте сообщения")
    void masksBannedWord() {
        bannedWords.pipeInput("дурак", "1");

        messages.pipeInput("carol", message("alice", "carol", "ты дурак, шучу :)"));

        ChatMessage result = filteredMessages.readValue();

        assertEquals("ты *****, шучу :)", result.message());
    }

    @Test
    @DisplayName("Удаление слова из справочника отменяет цензуру без перезапуска")
    void stopsMaskingAfterTombstone() {
        bannedWords.pipeInput("дурак", "1");
        messages.pipeInput("carol", message("alice", "carol", "дурак"));

        // Значение null — tombstone: слово снимается со списка.
        // Приведение обязательно: без него компилятор не различает перегрузки
        // pipeInput(ключ, значение) и pipeInput(значение, время).
        bannedWords.pipeInput("дурак", (String) null);
        messages.pipeInput("carol", message("alice", "carol", "дурак"));

        List<ChatMessage> result = filteredMessages.readValuesToList();

        assertEquals("*****", result.get(0).message());
        assertEquals("дурак", result.get(1).message(), "после удаления слово не маскируется");
    }

    @Test
    @DisplayName("Блокировка и цензура применяются вместе, в одном проходе")
    void appliesBothStages() {
        blockedUsers.pipeInput("bob", new BlockCommand("bob", "eve", "block"));
        bannedWords.pipeInput("дурак", "1");

        messages.pipeInput("bob", message("eve", "bob", "дурак"));       // отбрасывается
        messages.pipeInput("bob", message("alice", "bob", "дурак"));     // маскируется

        List<ChatMessage> result = filteredMessages.readValuesToList();

        assertEquals(1, result.size());
        assertEquals("alice", result.get(0).userId());
        assertEquals("*****", result.get(0).message());
    }

    @Test
    @DisplayName("Список блокировок лежит в state store и переживает обработку")
    void keepsBlockedUsersInStateStore() {
        blockedUsers.pipeInput("bob", new BlockCommand("bob", "eve", "block"));
        blockedUsers.pipeInput("bob", new BlockCommand("bob", "mallory", "block"));

        var store = driver.<String, java.util.Set<String>>getKeyValueStore(
                MessagingTopology.BLOCKED_USERS_STORE);

        assertTrue(store.get("bob").containsAll(List.of("eve", "mallory")));
    }

    private static ChatMessage message(String from, String to, String text) {
        return new ChatMessage(from, to, text, "2026-09-16T10:00:00Z");
    }
}
