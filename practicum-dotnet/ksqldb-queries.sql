-- Задание 2 (дополнительное). Анализ и агрегирование сообщений через ksqlDB.
--
-- Запросы выполняются по порядку в ksqlDB CLI:
--   docker compose --profile ksqldb up -d
--   docker exec -it practicum3-ksqldb-cli ksql http://ksqldb-server:8088
--
-- Проверить, что ksqlDB видит уже записанные сообщения:
SET 'auto.offset.reset' = 'earliest';

-- ---------------------------------------------------------------------------
-- 1. Поток поверх топика messages
-- ---------------------------------------------------------------------------
-- Топик уже существует и наполняется приложением, поэтому число партиций
-- здесь не задаётся: ksqlDB возьмёт его из существующего топика.
CREATE STREAM messages_stream (
    user_id VARCHAR,
    recipient_id VARCHAR,
    message VARCHAR,
    timestamp VARCHAR
) WITH (
    KAFKA_TOPIC = 'messages',
    VALUE_FORMAT = 'JSON'
);

-- ---------------------------------------------------------------------------
-- 2. Подзадача 1. Анализ сообщений в реальном времени
-- ---------------------------------------------------------------------------

-- Общее количество отправленных сообщений.
-- Агрегат по всему потоку требует группировки, поэтому все записи сводятся
-- в одну группу константным ключом.
CREATE TABLE total_messages AS
    SELECT
        1 AS metric_key,
        COUNT(*) AS total_messages
    FROM messages_stream
    GROUP BY 1
    EMIT CHANGES;

-- Количество уникальных получателей сообщений.
CREATE TABLE total_unique_recipients AS
    SELECT
        1 AS metric_key,
        COUNT_DISTINCT(recipient_id) AS unique_recipients
    FROM messages_stream
    GROUP BY 1
    EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- 3. Подзадача 2. Статистика по пользователям
-- ---------------------------------------------------------------------------

-- Обе метрики задания — в одной таблице: сколько сообщений отправил пользователь
-- и скольким разным получателям.
CREATE TABLE user_statistics AS
    SELECT
        user_id,
        COUNT(*) AS messages_sent,
        COUNT_DISTINCT(recipient_id) AS unique_recipients
    FROM messages_stream
    GROUP BY user_id
    EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- 4. Проверочные запросы
-- ---------------------------------------------------------------------------
-- Pull-запросы отдают текущее значение и сразу завершаются.

SELECT * FROM total_messages WHERE metric_key = 1;
SELECT * FROM total_unique_recipients WHERE metric_key = 1;
SELECT * FROM user_statistics WHERE user_id = 'alice';

-- Вся таблица статистики (push-запрос, прерывается по Ctrl+C):
SELECT * FROM user_statistics EMIT CHANGES;

-- Ожидаемый результат на тестовых данных из test-data/messages.jsonl:
--   total_messages          = 5
--   total_unique_recipients = 3   (bob, carol, alice)
--   user_statistics:
--     alice → messages_sent = 2, unique_recipients = 2  (bob, carol)
--     eve   → messages_sent = 1, unique_recipients = 1  (bob)
--     bob   → messages_sent = 1, unique_recipients = 1  (alice)
--     carol → messages_sent = 1, unique_recipients = 1  (alice)
--
-- Аналитика считается по топику messages, то есть по ВСЕМ отправленным сообщениям,
-- включая те, что потом отбросила блокировка. Это осознанно: статистика должна
-- отражать реальную активность отправителей. Чтобы считать только доставленное,
-- достаточно создать поток поверх filtered_messages.
