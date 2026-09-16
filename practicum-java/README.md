# Практическая работа 3. Блокировка и цензура сообщений на Kafka Streams

Сервис обмена сообщениями: пользователи блокируют нежелательных отправителей,
запрещённые слова маскируются. Списки блокировок и слов живут в persistent state
store и обновляются на лету, без перезапуска приложения. Реализация — Java 21,
[Apache Kafka Streams](https://kafka.apache.org/documentation/streams/) 3.7.

Та же топология на .NET (Streamiz) — в ветке
[`practicum-dotnet`](../../tree/practicum-dotnet/practicum-dotnet).
Термины (KStream, KTable, state store, changelog, tombstone) — в
[глоссарии модуля](../../blob/main/glossary.md).

## Содержание

- [Архитектура](#архитектура)
- [Запуск](#запуск)
- [Проверка работы](#проверка-работы)
- [Классы и параметры](#классы-и-параметры)
- [Задание 2. Аналитика через ksqlDB](#задание-2-аналитика-через-ksqldb)
- [Проверка соответствия заданию](#проверка-соответствия-заданию)
- [Как это работает](#как-это-работает)
- [Обработка ошибок](#обработка-ошибок)
- [Ограничения и компромиссы](#ограничения-и-компромиссы)

## Архитектура

```
   blocked_users                      messages                    banned_words
 (команды block/unblock)         (входящие сообщения)          (compacted, слово=ключ)
         │                                │                              │
         │ groupByKey + aggregate         │                              │ addGlobalStore
         ▼                                │                              ▼
 ┌───────────────────┐                    │                    ┌──────────────────┐
 │ blocked-users-    │                    │                    │ banned-words-    │
 │ store (RocksDB)   │◀───── leftJoin ────┤                    │ store (RocksDB,  │
 │ user → {кого он   │                    │                    │ глобальный)      │
 │ заблокировал}     │                    ▼                    └────────┬─────────┘
 └───────────────────┘             filter (≠ null)                      │
                                          │                             │
                                          ▼                             │
                                  processValues ◀──────────────────────-┘
                                  (маскирование слов)
                                          │
                                          ▼
                                  filtered_messages
```

Обе стадии — **один граф в одном процессе**. Промежуточного топика между блокировкой
и цензурой нет: это внутренний шаг топологии.

```
practicum-java/
├── docker-compose.yml       Kafka (KRaft) + приложение + ksqlDB + Kafka UI
├── ksqldb-queries.sql       задание 2: запросы аналитики
├── test-data/               тестовые сообщения, блокировки и слова
└── src/
    ├── build.gradle.kts
    ├── Dockerfile
    ├── main/java/ru/practicum/kafka/streams/
    │   ├── MessagingStreamsApp.java     точка входа
    │   ├── config/AppConfig.java        конфигурация из переменных окружения
    │   ├── model/                       ChatMessage, BlockCommand
    │   ├── serde/                       JSON-сериализация, включая Serde<Set<String>>
    │   ├── topology/                    MessagingTopology + два процессора
    │   └── util/WordMasker.java         маскирование слов
    └── test/java/                       тесты топологии и маскирования
```

## Запуск

Нужны Docker и Docker Compose v2. JDK 21 понадобится, только если запускать
приложение без Docker.

Порядок шагов важен: топики создаются **вручную**, автосоздание на брокере отключено.
Приложение вынесено в профиль `apps` и поэтому не стартует раньше времени.

### 1. Поднять брокер

```bash
cd practicum-java
docker compose up -d
docker compose ps          # дождаться статуса healthy у practicum3-kafka
```

### 2. Создать топики

```bash
docker exec -it practicum3-kafka bash -c '
  for t in messages filtered_messages blocked_users; do
    kafka-topics --create --topic $t --bootstrap-server kafka:29092 \
      --partitions 3 --replication-factor 1
  done

  # Справочник слов — compacted: Kafka хранит последнее значение каждого ключа,
  # поэтому полный актуальный список восстанавливается чтением топика с начала,
  # а запись со значением null (tombstone) убирает слово насовсем.
  kafka-topics --create --topic banned_words --bootstrap-server kafka:29092 \
    --partitions 3 --replication-factor 1 --config cleanup.policy=compact
'

docker exec -it practicum3-kafka kafka-topics --list --bootstrap-server kafka:29092
```

> Внутри контейнера адрес брокера — `kafka:29092` (внутренний листенер).
> С хост-машины — `localhost:9092`.

### 3. Загрузить справочники

Блокировки и запрещённые слова заливаются **до** сообщений, чтобы состояние успело
собраться. Формат строк в файлах — `ключ:значение`.

```bash
# Блокировки: bob блокирует eve
docker exec -i practicum3-kafka kafka-console-producer \
  --topic blocked_users --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/blocked_users.jsonl

# Запрещённые слова
docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/banned_words.txt
```

### 4. Запустить приложение

```bash
docker compose --profile apps up -d --build
docker compose logs -f messaging-streams-app
```

### 5. Отправить сообщения

```bash
docker exec -i practicum3-kafka kafka-console-producer \
  --topic messages --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/messages.jsonl
```

### 6. Посмотреть результат

```bash
docker exec -it practicum3-kafka kafka-console-consumer \
  --topic filtered_messages --bootstrap-server kafka:29092 \
  --from-beginning --property print.key=true
```

### Остановка

```bash
docker compose --profile apps rm -sf messaging-streams-app   # только приложение
docker compose down        # всё, данные в томах сохраняются
docker compose down -v     # всё вместе с данными и состоянием
```

## Проверка работы

Из пяти отправленных сообщений в `filtered_messages` приходят **четыре**:

| Отправитель | Получатель | Что происходит |
|---|---|---|
| alice | bob | доходит без изменений |
| **eve** | **bob** | **отброшено** — bob заблокировал eve |
| alice | carol | доходит, «дурак» → `*****` |
| bob | alice | доходит без изменений |
| carol | alice | доходит без изменений |

### Динамическое обновление списка слов

Список меняется без перезапуска приложения. Добавить слово:

```bash
echo "шучу:1" | docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=:
```

Убрать слово — записью с пустым значением (tombstone):

```bash
echo "дурак:" | docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  --property null.marker=  --property parse.null=true
```

После любой из этих команд повторно отправьте сообщение с этим словом и сравните
результат в `filtered_messages`.

### Проверка блокировки в динамике

```bash
# carol блокирует alice
echo 'carol:{"blocker_id":"carol","blocked_id":"alice","action":"block"}' | \
  docker exec -i practicum3-kafka kafka-console-producer \
    --topic blocked_users --bootstrap-server kafka:29092 \
    --property parse.key=true --property key.separator=:

# Следующее сообщение alice → carol уже не дойдёт
```

### Проверка персистентности состояния

```bash
docker compose --profile apps restart messaging-streams-app
```

После перезапуска блокировки и список слов действуют **без повторной заливки**:
состояние лежит на диске в томе и восстанавливается из changelog-топиков.

### Автотесты

Топология проверяется без брокера через `TopologyTestDriver`:

```bash
cd src
docker run --rm -v "$(pwd):/work" -w /work gradle:8.7-jdk21 gradle --no-daemon test
```

11 тестов: маскирование слов (5) и топология целиком (6) — блокировка,
разблокировка, цензура, tombstone, обе стадии вместе, содержимое state store.

## Классы и параметры

| Класс | Назначение |
|---|---|
| `MessagingStreamsApp` | Точка входа: собирает топологию, запускает `KafkaStreams`, обрабатывает сигнал завершения. |
| `MessagingTopology` | Весь граф обработки: таблица блокировок, глобальный справочник слов, join, фильтр, цензура. |
| `BannedWordsUpdater` | Процессор глобального хранилища: пишет слово в справочник, по tombstone удаляет. Нормализует регистр. |
| `CensorshipProcessor` | `FixedKeyProcessor`: читает справочник и маскирует слова в тексте. Ключ не меняет. |
| `WordMasker` | Чистая функция маскирования. Вынесена отдельно, чтобы проверяться юнит-тестом. |
| `ChatMessage`, `BlockCommand` | Модели сообщения и команды блокировки. |
| `JsonSerde`, `AppSerdes` | JSON-сериализация через Jackson; ошибка разбора логируется и даёт `null`. |
| `AppConfig` | Настройки из переменных окружения с значениями по умолчанию для запуска без Docker. |

### Хранилища состояния

| Store | Что хранит | Тип |
|---|---|---|
| `blocked-users-store` | `user → множество заблокированных им` | persistent (RocksDB), с changelog-топиком |
| `banned-words-store` | `слово → маркер` | persistent, глобальный (полная копия на каждом экземпляре) |

### Переменные окружения

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | адреса брокеров |
| `KAFKA_APPLICATION_ID` | `practicum3-messaging-streams` | `application.id`, он же `group.id` |
| `KAFKA_STATE_DIR` | `/tmp/practicum3-state` | каталог persistent state store |
| `KAFKA_TOPIC_MESSAGES` | `messages` | входящие сообщения |
| `KAFKA_TOPIC_FILTERED_MESSAGES` | `filtered_messages` | сообщения после обработки |
| `KAFKA_TOPIC_BLOCKED_USERS` | `blocked_users` | команды блокировки |
| `KAFKA_TOPIC_BANNED_WORDS` | `banned_words` | справочник запрещённых слов |
| `LOG_LEVEL` | `INFO` | уровень логирования |

## Задание 2. Аналитика через ksqlDB

Дополнительное задание, поднимается отдельным профилем.

```bash
docker compose --profile ksqldb up -d
docker exec -it practicum3-ksqldb-cli ksql http://ksqldb-server:8088
```

Дальше выполнить запросы из [ksqldb-queries.sql](ksqldb-queries.sql) по порядку.
Создаются поток `messages_stream` и три таблицы:

| Таблица | Что считает |
|---|---|
| `total_messages` | общее количество отправленных сообщений |
| `total_unique_recipients` | количество уникальных получателей |
| `user_statistics` | на каждого пользователя: сколько сообщений отправил и скольким разным получателям |

Обе метрики задания по пользователям уместились в одну таблицу `user_statistics`:
`COUNT(*)` и `COUNT_DISTINCT(recipient_id)` считаются в одном `GROUP BY`.

Ожидаемый результат на тестовых данных приведён в конце файла запросов.

## Проверка соответствия заданию

### Задание 1

| # | Требование | Как проверить | Ожидаемый результат |
|---|---|---|---|
| 1 | Библиотека потоковой обработки | `src/build.gradle.kts` | зависимость `org.apache.kafka:kafka-streams` |
| 2 | Отдельный список заблокированных для каждого пользователя | `MessagingTopology.buildBlockedUsersTable` | `KTable<user, Set<blocked>>` — одна запись на пользователя |
| 3 | Список хранится в persistent state store | там же | `Materialized.as(Stores.persistentKeyValueStore("blocked-users-store"))` |
| 4 | Сообщения от заблокированных не доходят | шаги 3–6 запуска | сообщения от `eve` нет в `filtered_messages`, в логе `Сообщение отброшено` |
| 5 | Список запрещённых слов обновляется динамически | [раздел выше](#динамическое-обновление-списка-слов) | новое слово маскируется без перезапуска; tombstone отменяет маскирование |
| 6 | Компонент, удаляющий или маскирующий слова | `CensorshipProcessor` + `WordMasker` | «дурак» → `*****` |
| 7 | Все сообщения проходят цензуру | `MessagingTopology.build` | `processValues` стоит на пути каждого сообщения к `filtered_messages` |
| 8 | Docker-compose разворачивает систему | `docker compose up -d` | брокер `healthy`, приложение `Up` |
| 9 | Топики `messages`, `filtered_messages`, `blocked_users` | шаг 2 запуска | `kafka-topics --list` показывает все три (плюс `banned_words`) |
| 10 | Тестовые данные | `test-data/` | три файла, команды заливки в шагах 3 и 5 |
| 11 | Логичная структура и понятные комментарии | `src/main/java/` | разделение на модель, сериализацию, топологию, утилиты |
| 12 | Состояние переживает перезапуск | [раздел выше](#проверка-персистентности-состояния) | после `restart` блокировки действуют без повторной заливки |

### Задание 2

| # | Требование | Как проверить | Ожидаемый результат |
|---|---|---|---|
| 1 | Поток `messages_stream` | запрос 1 из `ksqldb-queries.sql` | `SHOW STREAMS;` содержит `MESSAGES_STREAM` |
| 2 | Общее количество сообщений | `SELECT * FROM total_messages WHERE metric_key = 1;` | `5` на тестовых данных |
| 3 | Количество уникальных получателей | `SELECT * FROM total_unique_recipients WHERE metric_key = 1;` | `3` |
| 4 | Сообщения каждого пользователя | `SELECT * FROM user_statistics WHERE user_id = 'alice';` | `messages_sent = 2` |
| 5 | Уникальные получатели каждого пользователя | там же | `unique_recipients = 2` |
| 6 | Таблица `user_statistics` | `SHOW TABLES;` | таблица создана |

## Как это работает

### Почему ключ сообщения — получатель

Сообщение отбрасывается, если **получатель** заблокировал **отправителя**. Значит,
для каждого сообщения нужно достать список блокировок его получателя — то есть
соединить поток с таблицей по `recipient_id`.

Kafka Streams выполняет такое соединение локально, только если у обоих входов
одинаковая схема ключей: связанные записи должны лежать в одной партиции. Поэтому
ключ записи в `messages` — `recipient_id`, а ключ таблицы блокировок — `blocker_id`,
и это один и тот же пользователь. Условие совмещения выполняется само собой,
дополнительный repartition-топик не нужен.

### Почему команды, а не готовый список

В топик `blocked_users` кладутся команды `block`/`unblock`, а не полный список
блокировок пользователя. Иначе клиенту пришлось бы знать текущий список целиком
перед каждым изменением, а параллельные блокировки от разных клиентов затирали бы
друг друга.

Топология сворачивает поток команд в множество (`groupByKey().aggregate(...)`) —
это и есть тот самый «отдельный список заблокированных для каждого пользователя»
из задания, лежащий в persistent state store.

### Почему `leftJoin` + `filter`, а не просто `filter`

В DSL нет операции «отбросить запись, если в таблице что-то нашлось». Штатный приём:
`leftJoin` вызывает объединитель для каждой записи потока (даже когда в таблице
пусто), объединитель возвращает `null` для заблокированных, а следующий `filter`
эти `null` отсеивает.

Именно `leftJoin`, а не `join`: обычный `join` пропустил бы сообщения тем
пользователям, у которых вообще нет списка блокировок.

### Почему справочник слов — глобальное хранилище

Запрещённые слова не связаны с ключом сообщения: для маскирования нужно проверить
**каждое слово текста**, а не одно значение по ключу записи. Обычная KTable здесь
не подходит — она делится между экземплярами приложения по партициям, и нужного
слова могло бы просто не оказаться на этом экземпляре.

`addGlobalStore` даёт каждому экземпляру полную копию справочника. Использован он,
а не `globalTable`, потому что цензору нужен прямой доступ к хранилищу для многих
обращений на одно сообщение, а не DSL-соединение по одному ключу.

Важная деталь: имя глобального хранилища **не передаётся** в `processValues` третьим
аргументом. Процессоры получают доступ к глобальным хранилищам автоматически, а явное
подключение вызвало бы ошибку `StateStore ... is not added yet`.

### Почему compacted-топик и tombstone

Справочник слов — это состояние, а не поток событий: важно текущее содержимое списка,
а не история его изменений. Compacted-топик хранит последнее значение каждого ключа,
поэтому список полностью восстанавливается чтением топика с начала, сколько бы
обновлений ни накопилось.

Удаление слова — запись со значением `null` (tombstone). Это штатная семантика Kafka:
альтернативой была бы команда вида `{"word":"...","action":"remove"}`, но тогда
пришлось бы вручную писать логику вычитания, а compacted-топик рос бы вечно.

### Почему `processValues`, а не `transformValues`

`transformValues` объявлен устаревшим в Kafka Streams 3.3 (KIP-820). Замена —
`processValues` с типобезопасным `FixedKeyProcessor`, который на уровне типов
запрещает менять ключ записи. Благодаря этому Kafka Streams знает, что
партиционирование сохранилось, и не вставляет лишний repartition-топик.

### Порядок стадий

Сначала блокировка, потом цензура: проверка вхождения в множество дешевле разбора
текста, и нет смысла цензурировать сообщение, которое всё равно будет отброшено.

## Обработка ошибок

| Ситуация | Поведение |
|---|---|
| Битый JSON во входящем сообщении | `JsonSerde` пишет ошибку в лог и возвращает `null`; запись отсеивается фильтром, обработка продолжается |
| Битая команда блокировки | то же: отсеивается до аккумулятора, состояние не портится |
| Запись справочника без ключа | предупреждение в лог, запись пропускается |
| Сообщение без текста | проходит дальше без цензуры, не роняя топологию |
| Необработанное исключение в потоке обработки | `REPLACE_THREAD`: поток заменяется новым, приложение продолжает работать |
| Недоступен брокер | клиент Kafka переподключается сам; при старте приложение ждёт брокера |
| `docker stop` | `close()` в shutdown-хуке досылает буферы, коммитит смещения и выходит из группы |

В продакшене неразбираемые сообщения уходили бы в отдельный топик (Dead Letter Queue),
здесь по заданию достаточно записи в лог.

## Ограничения и компромиссы

**Топик `banned_words` добавлен сверх списка задания.** Задание перечисляет `messages`,
`filtered_messages` и `blocked_users`, но требует «динамически обновляемый список
запрещённых слов». Список должен где-то храниться и обновляться без перезапуска —
топик единственный способ это сделать в рамках Kafka. Захардкоженный список требование
о динамике не выполнял бы.

**Один брокер вместо кластера.** В первом модуле поднимались три брокера с репликацией.
Здесь фокус на потоковой обработке, а не на кластерных гарантиях, поэтому брокер один
и в режиме KRaft (без ZooKeeper). Следствие: `replication.factor=1` у всех топиков,
включая служебные, отказоустойчивости на уровне брокера нет.

**Маскирование вместо удаления.** Задание допускает и то, и другое. Выбрано
маскирование звёздочками: получателю видно, что слово было вырезано, а длина текста
не меняется.

**Цензура работает по отдельным словам.** Запрещённое слово внутри другого слова
не маскируется («дурака» останется как есть). Это осознанно: поиск по подстроке
даёт ложные срабатывания на безобидных словах. Обход фильтра написанием через
пробелы или похожие символы здесь не рассматривается — это отдельная большая задача.

**Аналитика ksqlDB считает по топику `messages`.** То есть по всем отправленным
сообщениям, включая отброшенные блокировкой. Так статистика отражает реальную
активность отправителей; для подсчёта только доставленного достаточно создать поток
поверх `filtered_messages`.

**Что можно улучшить.** Схемы данных через Schema Registry, Dead Letter Queue для
неразбираемых сообщений, exactly-once семантика (`processing.guarantee=exactly_once_v2`),
Interactive Queries для просмотра состояния по HTTP, ограничение размера множества
блокировок на пользователя.
