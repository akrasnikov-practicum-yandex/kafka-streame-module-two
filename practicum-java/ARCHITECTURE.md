# Структура проекта и поток обработки

Разбор кодовой базы: какие файлы за что отвечают, что делает каждый класс и метод,
и как требования задания превращаются в конкретные вызовы.

Запуск — в [QUICKSTART.md](QUICKSTART.md). Обоснование архитектурных решений
(«почему leftJoin», «почему глобальное хранилище») — в [README.md](README.md).
Термины — в [глоссарии модуля](../../blob/main/glossary.md).

## Содержание

- [Карта файлов](#карта-файлов)
- [C4: контекст (Level 1)](#c4-контекст-level-1)
- [C4: контейнеры (Level 2)](#c4-контейнеры-level-2)
- [C4: компоненты (Level 3)](#c4-компоненты-level-3)
- [Слои и зависимости](#слои-и-зависимости)
- [Классы и методы](#классы-и-методы)
  - [MessagingStreamsApp](#messagingstreamsapp)
  - [AppConfig](#appconfig)
  - [MessagingTopology](#messagingtopology)
  - [BannedWordsUpdater](#bannedwordsupdater)
  - [CensorshipProcessor](#censorshipprocessor)
  - [WordMasker](#wordmasker)
  - [ChatMessage](#chatmessage)
  - [BlockCommand](#blockcommand)
  - [JsonSerde](#jsonserde)
  - [AppSerdes](#appserdes)
- [Sequence-диаграммы](#sequence-диаграммы)
  - [Старт приложения](#1-старт-приложения)
  - [Обновление справочника слов](#2-обновление-справочника-слов)
  - [Команда блокировки](#3-команда-блокировки)
  - [Главный поток: обработка сообщения](#4-главный-поток-обработка-сообщения)
  - [Сообщение от заблокированного отправителя](#5-сообщение-от-заблокированного-отправителя)
  - [Удаление слова через tombstone](#6-удаление-слова-через-tombstone)
  - [Битый JSON](#7-битый-json)
  - [Перезапуск и восстановление состояния](#8-перезапуск-и-восстановление-состояния)
- [Привязка требований задания к коду](#привязка-требований-задания-к-коду)
- [Состояние: где что лежит](#состояние-где-что-лежит)
- [Тесты](#тесты)

---

## Карта файлов

```
practicum-java/
├── docker-compose.yml                     стенд: Kafka (KRaft), приложение, ksqlDB, Kafka UI
├── ksqldb-queries.sql                     задание 2: поток + три таблицы аналитики
├── QUICKSTART.md                          пошаговый запуск
├── README.md                              архитектура и обоснование решений
├── ARCHITECTURE.md                        этот документ
├── test-data/
│   ├── messages.jsonl                     5 сообщений, ключ = recipient_id
│   ├── blocked_users.jsonl                1 команда: bob блокирует eve
│   └── banned_words.txt                   2 слова: дурак, идиот
└── src/
    ├── build.gradle.kts                   Java 21, kafka-streams 3.7.0, Jackson, Logback
    ├── settings.gradle.kts                rootProject.name = practicum-messaging-streams
    ├── Dockerfile                         многоступенчатая сборка: gradle:8.7-jdk21 → temurin:21-jre
    ├── main/
    │   ├── java/ru/practicum/kafka/streams/
    │   │   ├── MessagingStreamsApp.java   точка входа, жизненный цикл KafkaStreams
    │   │   ├── config/
    │   │   │   └── AppConfig.java         конфигурация из переменных окружения
    │   │   ├── model/
    │   │   │   ├── ChatMessage.java       сообщение чата
    │   │   │   └── BlockCommand.java      команда block/unblock
    │   │   ├── serde/
    │   │   │   ├── JsonSerde.java         универсальная JSON-сериализация
    │   │   │   └── AppSerdes.java         фабрика готовых Serde
    │   │   ├── topology/
    │   │   │   ├── MessagingTopology.java граф обработки целиком
    │   │   │   ├── BannedWordsUpdater.java процессор глобального хранилища
    │   │   │   └── CensorshipProcessor.java цензор текста
    │   │   └── util/
    │   │       └── WordMasker.java        чистая функция маскирования
    │   └── resources/
    │       └── logback.xml                настройка логирования
    └── test/java/ru/practicum/kafka/streams/
        ├── topology/MessagingTopologyTest.java   6 тестов через TopologyTestDriver
        └── util/WordMaskerTest.java              5 тестов маскирования
```

> **Нестандартная раскладка Gradle.** Корень Gradle-проекта — сама папка `src/`,
> поэтому исходники лежат в `src/main/java`, а не в `src/src/main/java`. Это задано
> явно в `build.gradle.kts` через `sourceSets`.

---

## C4: контекст (Level 1)

Система целиком и всё, что с ней взаимодействует. Внутреннее устройство на этом
уровне намеренно скрыто — видно только границу.

```mermaid
graph TB
    subgraph ext[" "]
        User["👤 <b>Пользователь чата</b><br/><i>[Person]</i><br/>Отправляет сообщения,<br/>блокирует нежелательных отправителей"]
        Moder["👤 <b>Модератор</b><br/><i>[Person]</i><br/>Ведёт список запрещённых слов"]
        Analyst["👤 <b>Аналитик</b><br/><i>[Person]</i><br/>Смотрит статистику переписки"]
    end

    System["<b>Сервис обмена сообщениями</b><br/><i>[Software System]</i><br/>Доставляет сообщения между пользователями,<br/>отсеивает заблокированных отправителей<br/>и маскирует запрещённые слова"]

    User -->|"Отправляет сообщения<br/>и команды block/unblock"| System
    Moder -->|"Добавляет и удаляет<br/>запрещённые слова"| System
    System -->|"Доставляет отфильтрованные<br/>и отцензурированные сообщения"| User
    Analyst -->|"Запрашивает агрегаты<br/>по потоку сообщений"| System

    style System fill:#1168bd,stroke:#0b4884,color:#fff
    style User fill:#08427b,stroke:#052e56,color:#fff
    style Moder fill:#08427b,stroke:#052e56,color:#fff
    style Analyst fill:#08427b,stroke:#052e56,color:#fff
    style ext fill:none,stroke:none
```

В учебном стенде все три роли играет один человек через консольные утилиты Kafka:
пользователь и модератор — `kafka-console-producer`, аналитик — `ksqlDB CLI`.
Отдельного клиентского приложения в работе нет, оно вне границ задания.

## C4: контейнеры (Level 2)

Из чего система собрана и как части общаются. Каждый контейнер — отдельный
процесс в `docker-compose.yml`; топики Kafka показаны как хранилища данных,
потому что для этой системы они и есть persistent-слой.

```mermaid
graph TB
    User["👤 <b>Пользователь / Модератор</b><br/><i>[Person]</i>"]
    Analyst["👤 <b>Аналитик</b><br/><i>[Person]</i>"]

    subgraph boundary["Сервис обмена сообщениями"]
        direction TB

        App["<b>messaging-streams-app</b><br/><i>[Container: Java 21 + Kafka Streams 3.7]</i><br/>Блокировка отправителей и цензура слов.<br/>Один граф обработки в одном процессе"]

        subgraph stores["Локальное состояние (RocksDB)"]
            BStore[("<b>blocked-users-store</b><br/><i>[Container: RocksDB]</i><br/>user → множество<br/>заблокированных им")]
            WStore[("<b>banned-words-store</b><br/><i>[Container: RocksDB, global]</i><br/>слово → маркер")]
        end

        Broker["<b>Kafka broker</b><br/><i>[Container: confluentinc/cp-kafka 7.6, KRaft]</i><br/>Транспорт и хранение всех топиков"]

        subgraph topics["Топики"]
            TMsg[("messages<br/><i>key = recipient_id</i>")]
            TFiltered[("filtered_messages<br/><i>результат обработки</i>")]
            TBlocked[("blocked_users<br/><i>команды block/unblock</i>")]
            TWords[("banned_words<br/><i>compacted, слово = ключ</i>")]
        end

        Ksql["<b>ksqlDB</b><br/><i>[Container: cp-ksqldb-server 7.6]</i><br/>Агрегаты по потоку сообщений<br/><i>профиль ksqldb, задание 2</i>"]
        UI["<b>Kafka UI</b><br/><i>[Container: provectuslabs/kafka-ui]</i><br/>Просмотр топиков, групп и лага<br/><i>профиль tools, необязательный</i>"]
    end

    User -->|"Пишет записи<br/><i>kafka-console-producer</i>"| Broker
    Broker -->|"Читает результат<br/><i>kafka-console-consumer</i>"| User
    Analyst -->|"SQL-запросы<br/><i>ksqlDB CLI, HTTP :8088</i>"| Ksql

    Broker --- topics

    TMsg -->|"читает<br/><i>Kafka protocol</i>"| App
    TBlocked -->|"читает<br/><i>Kafka protocol</i>"| App
    TWords -->|"читает<br/><i>Kafka protocol</i>"| App
    App -->|"пишет<br/><i>Kafka protocol</i>"| TFiltered

    App -->|"читает и пишет<br/><i>локально</i>"| BStore
    App -->|"читает и пишет<br/><i>локально</i>"| WStore
    BStore -.->|"changelog-топик<br/><i>восстановление</i>"| Broker

    Ksql -->|"читает messages<br/><i>Kafka protocol</i>"| Broker
    UI -->|"метаданные и записи<br/><i>Kafka protocol</i>"| Broker

    style App fill:#1168bd,stroke:#0b4884,color:#fff
    style Broker fill:#1168bd,stroke:#0b4884,color:#fff
    style Ksql fill:#4b8fd4,stroke:#0b4884,color:#fff
    style UI fill:#7fa8d4,stroke:#0b4884,color:#fff
    style BStore fill:#438dd5,stroke:#2e6295,color:#fff
    style WStore fill:#438dd5,stroke:#2e6295,color:#fff
    style TMsg fill:#438dd5,stroke:#2e6295,color:#fff
    style TFiltered fill:#438dd5,stroke:#2e6295,color:#fff
    style TBlocked fill:#438dd5,stroke:#2e6295,color:#fff
    style TWords fill:#438dd5,stroke:#2e6295,color:#fff
    style User fill:#08427b,stroke:#052e56,color:#fff
    style Analyst fill:#08427b,stroke:#052e56,color:#fff
    style boundary fill:none,stroke:#0b4884,stroke-dasharray: 5 5
    style stores fill:none,stroke:#999,stroke-dasharray: 3 3
    style topics fill:none,stroke:#999,stroke-dasharray: 3 3
```

Что важно прочитать с этой диаграммы:

| Наблюдение | Почему так |
|---|---|
| Между `messages` и `filtered_messages` нет промежуточного топика | Блокировка и цензура — два шага **одного** графа в одном процессе, а не два сервиса |
| `banned_words` — единственный compacted-топик | Это состояние, а не поток событий: важно текущее содержимое списка, а не история |
| `banned-words-store` помечен `global` | Каждый экземпляр приложения держит полную копию справочника |
| У `blocked-users-store` есть changelog, у `banned-words-store` — нет | Первый восстанавливается из служебного changelog-топика, второй перечитывается из исходного `banned_words` |
| ksqlDB читает `messages`, а не `filtered_messages` | Статистика считается по всем отправленным сообщениям, включая отброшенные блокировкой |
| ksqlDB и Kafka UI поднимаются профилями | Не нужны для задания 1; стенд по умолчанию — только брокер |

Порты наружу: `9092` (брокер, внешний листенер), `8088` (ksqlDB), `8080` (Kafka UI).
Внутри docker-сети брокер доступен как `kafka:29092`.

## C4: компоненты (Level 3)

Устройство контейнера `messaging-streams-app` изнутри: из каких компонентов он
состоит, кто кого вызывает и через какие узлы топологии проходит запись.

```mermaid
graph TB
    TMsg[("<b>messages</b><br/><i>[Topic]</i>")]
    TBlocked[("<b>blocked_users</b><br/><i>[Topic]</i>")]
    TWords[("<b>banned_words</b><br/><i>[Topic, compacted]</i>")]
    TOut[("<b>filtered_messages</b><br/><i>[Topic]</i>")]

    subgraph app["Контейнер: messaging-streams-app"]
        direction TB

        Main["<b>MessagingStreamsApp</b><br/><i>[Component: main]</i><br/>Жизненный цикл KafkaStreams:<br/>старт, обработчик исключений,<br/>shutdown-hook"]
        Cfg["<b>AppConfig</b><br/><i>[Component: record]</i><br/>Конфигурация из переменных<br/>окружения → Properties"]

        subgraph topo["Топология обработки"]
            direction TB
            Builder["<b>MessagingTopology</b><br/><i>[Component: factory]</i><br/>Сборка графа:<br/>leftJoin → filter → processValues → to"]
            Agg["<b>applyBlockCommand</b><br/><i>[Component: aggregator]</i><br/>Свёртка команд<br/>в множество"]
            Joiner["<b>dropIfBlocked</b><br/><i>[Component: ValueJoiner]</i><br/>null, если отправитель<br/>заблокирован получателем"]
            BWU["<b>BannedWordsUpdater</b><br/><i>[Component: Processor]</i><br/>put / delete по tombstone,<br/>нормализация регистра"]
            CP["<b>CensorshipProcessor</b><br/><i>[Component: FixedKeyProcessor]</i><br/>Читает справочник,<br/>маскирует текст, не меняет ключ"]
        end

        WM["<b>WordMasker</b><br/><i>[Component: pure function]</i><br/>Замена слов звёздочками<br/>по границам слов Unicode"]

        subgraph ser["Сериализация"]
            direction TB
            Serdes["<b>AppSerdes</b><br/><i>[Component: factory]</i><br/>chatMessage, blockCommand,<br/>stringSet"]
            Json["<b>JsonSerde&lt;T&gt;</b><br/><i>[Component: Serde]</i><br/>Jackson; ошибка разбора<br/>→ лог + null"]
        end

        subgraph mdl["Модели"]
            direction TB
            Msg["<b>ChatMessage</b><br/><i>[Component: record]</i><br/>user_id, recipient_id,<br/>message, timestamp"]
            Cmd["<b>BlockCommand</b><br/><i>[Component: record]</i><br/>blocker_id, blocked_id,<br/>action"]
        end

        BStore[("<b>blocked-users-store</b><br/><i>[RocksDB]</i>")]
        WStore[("<b>banned-words-store</b><br/><i>[RocksDB, global]</i>")]
    end

    Main -->|"fromEnvironment()<br/>toStreamsProperties()"| Cfg
    Main -->|"build(config)"| Builder
    Builder -->|"читает имена топиков"| Cfg

    TBlocked -->|"Consumed.with"| Json
    Json -->|"BlockCommand"| Agg
    Agg -->|"put(user, Set)"| BStore
    Agg -.->|"использует"| Cmd

    TWords -->|"addGlobalStore"| BWU
    BWU -->|"put / delete"| WStore

    TMsg -->|"Consumed.with"| Json
    Json -->|"ChatMessage"| Joiner
    BStore -->|"get(recipientId)"| Joiner
    Joiner -->|"не-null записи"| CP
    Joiner -.->|"использует"| Msg

    CP -->|"mask(text, isBanned)"| WM
    WM -->|"get(word)"| WStore
    CP -->|"forward → Produced.with"| Json
    Json -->|"bytes"| TOut

    Builder -.->|"создаёт"| Joiner
    Builder -.->|"создаёт"| Agg
    Builder -.->|"создаёт"| BWU
    Builder -.->|"создаёт"| CP
    Builder -->|"Serde для узлов"| Serdes
    Serdes -->|"new JsonSerde&lt;&gt;"| Json

    style Main fill:#1168bd,stroke:#0b4884,color:#fff
    style Builder fill:#1168bd,stroke:#0b4884,color:#fff
    style CP fill:#438dd5,stroke:#2e6295,color:#fff
    style BWU fill:#438dd5,stroke:#2e6295,color:#fff
    style Joiner fill:#438dd5,stroke:#2e6295,color:#fff
    style Agg fill:#438dd5,stroke:#2e6295,color:#fff
    style WM fill:#85bbf0,stroke:#2e6295,color:#111
    style Cfg fill:#85bbf0,stroke:#2e6295,color:#111
    style Serdes fill:#85bbf0,stroke:#2e6295,color:#111
    style Json fill:#85bbf0,stroke:#2e6295,color:#111
    style Msg fill:#b3d2f2,stroke:#2e6295,color:#111
    style Cmd fill:#b3d2f2,stroke:#2e6295,color:#111
    style BStore fill:#438dd5,stroke:#2e6295,color:#fff
    style WStore fill:#438dd5,stroke:#2e6295,color:#fff
    style TMsg fill:#999,stroke:#666,color:#fff
    style TBlocked fill:#999,stroke:#666,color:#fff
    style TWords fill:#999,stroke:#666,color:#fff
    style TOut fill:#999,stroke:#666,color:#fff
    style app fill:none,stroke:#0b4884,stroke-dasharray: 5 5
    style topo fill:none,stroke:#999,stroke-dasharray: 3 3
    style ser fill:none,stroke:#999,stroke-dasharray: 3 3
    style mdl fill:none,stroke:#999,stroke-dasharray: 3 3
```

Сплошные стрелки — поток данных во время работы, пунктирные — связи времени
сборки графа и использования типов.

Три входа в топологию независимы: `blocked_users` наполняет таблицу блокировок,
`banned_words` — глобальный справочник, и только `messages` идёт по основному
конвейеру, читая оба хранилища по пути. Выход один — `filtered_messages`.

`WordMasker` — единственный компонент, не связанный с Kafka ни одним типом:
поэтому он и вынесен отдельно, его проверяет обычный юнит-тест без топологии.

Динамика этих же компонентов — в [sequence-диаграммах](#sequence-диаграммы).

---

## Слои и зависимости

```mermaid
graph TD
    App[MessagingStreamsApp<br/>точка входа]
    Config[AppConfig<br/>конфигурация]
    Topo[MessagingTopology<br/>граф обработки]
    BWU[BannedWordsUpdater<br/>процессор справочника]
    CP[CensorshipProcessor<br/>цензор]
    WM[WordMasker<br/>чистая функция]
    Serdes[AppSerdes / JsonSerde<br/>сериализация]
    Model[ChatMessage / BlockCommand<br/>модели]

    App --> Config
    App --> Topo
    Topo --> BWU
    Topo --> CP
    Topo --> Serdes
    Topo --> Config
    CP --> WM
    CP --> Model
    Serdes --> Model

    style App fill:#e8f0fe,stroke:#4285f4,color:#111
    style Topo fill:#e6f4ea,stroke:#34a853,color:#111
    style WM fill:#fef7e0,stroke:#fbbc04,color:#111
```

Зависимости направлены в одну сторону, циклов нет. `WordMasker` не зависит ни от
чего из Kafka — поэтому проверяется обычным юнит-тестом.

---

## Классы и методы

### MessagingStreamsApp

`ru.practicum.kafka.streams.MessagingStreamsApp` — точка входа. Вся логика
обработки вынесена в топологию, здесь только жизненный цикл.

| Метод | Сигнатура | Что делает |
|---|---|---|
| `main` | `static void main(String[] args)` | Читает конфигурацию, строит топологию, запускает `KafkaStreams` и блокируется до сигнала завершения. |

Что происходит внутри `main` по шагам:

1. `AppConfig.fromEnvironment()` — конфигурация из переменных окружения.
2. `MessagingTopology.build(config)` — построение графа.
3. `log.info("Топология:\n{}", topology.describe())` — печать фактического плана
   выполнения: источники, узлы, хранилища, стоки. По этому выводу видно, вставил
   ли Kafka Streams repartition-топик.
4. `setUncaughtExceptionHandler` → `REPLACE_THREAD` — необработанное исключение
   заменяет поток обработки новым, приложение продолжает работать.
5. `setStateListener` — лог перехода состояний (`CREATED → REBALANCING → RUNNING`).
6. `addShutdownHook` — на `SIGTERM` от `docker stop` вызывается `streams.close()`:
   досылает буферы, коммитит смещения, выходит из группы. Без этого партиции
   освободились бы только по таймауту сессии.
7. `streams.start()` и `shutdownLatch.await()` — main-поток ждёт, обработка идёт
   в потоках Kafka Streams.

`CountDownLatch` нужен, потому что `streams.start()` не блокирует: без ожидания
`main` завершился бы сразу после старта.

### AppConfig

`ru.practicum.kafka.streams.config.AppConfig` — **record** с семью полями:
`bootstrapServers`, `applicationId`, `stateDir`, `messagesTopic`,
`filteredMessagesTopic`, `blockedUsersTopic`, `bannedWordsTopic`.

| Метод | Сигнатура | Что делает |
|---|---|---|
| `fromEnvironment` | `static AppConfig fromEnvironment()` | Собирает конфигурацию из переменных окружения; для каждой есть значение по умолчанию для запуска без Docker. |
| `toStreamsProperties` | `Properties toStreamsProperties()` | Превращает конфигурацию в `Properties` для `KafkaStreams`. |
| `env` | `private static String env(String name, String defaultValue)` | Читает переменную; пустая или отсутствующая заменяется значением по умолчанию. |

Что именно кладётся в `toStreamsProperties()` и почему:

| Настройка | Значение | Зачем |
|---|---|---|
| `APPLICATION_ID_CONFIG` | из `KAFKA_APPLICATION_ID` | он же `group.id` и префикс служебных топиков и каталога состояния |
| `BOOTSTRAP_SERVERS_CONFIG` | из `KAFKA_BOOTSTRAP_SERVERS` | адреса брокеров |
| `STATE_DIR_CONFIG` | из `KAFKA_STATE_DIR` | каталог RocksDB; в контейнере примонтирован volume |
| `DEFAULT_KEY/VALUE_SERDE` | `Serdes.String()` | требуется для служебных операций; везде, где тип важен, Serde задан явно |
| `AUTO_OFFSET_RESET_CONFIG` | `earliest` | иначе приложение не увидит блокировки и слова, записанные до первого запуска |
| `REPLICATION_FACTOR_CONFIG` | `1` | брокер один, реплицировать changelog не на что |

### MessagingTopology

`ru.practicum.kafka.streams.topology.MessagingTopology` — весь граф обработки.
Класс `final`, конструктор приватный: только статические методы.

Константы:

| Константа | Значение |
|---|---|
| `BLOCKED_USERS_STORE` | `"blocked-users-store"` |
| `BANNED_WORDS_STORE` | `"banned-words-store"` |

| Метод | Сигнатура | Что делает |
|---|---|---|
| `build` | `public static Topology build(AppConfig config)` | Собирает граф целиком и возвращает `Topology`. Единственный публичный метод. |
| `buildBlockedUsersTable` | `private static KTable<String, Set<String>> buildBlockedUsersTable(StreamsBuilder, AppConfig)` | Сворачивает поток команд `block`/`unblock` в таблицу «пользователь → множество заблокированных им». |
| `addBannedWordsStore` | `private static void addBannedWordsStore(StreamsBuilder, AppConfig)` | Регистрирует глобальное хранилище справочника слов с процессором `BannedWordsUpdater`. |
| `applyBlockCommand` | `private static Set<String> applyBlockCommand(String blockerId, BlockCommand command, Set<String> blocked)` | Аккумулятор таблицы: `block` добавляет в множество, `unblock` убирает. |
| `dropIfBlocked` | `private static ChatMessage dropIfBlocked(ChatMessage message, Set<String> blockedByRecipient)` | Объединитель `leftJoin`: возвращает `null`, если отправитель в списке заблокированных получателя. |

Цепочка вызовов внутри `build`:

```java
messages
    .leftJoin(blockedUsers, MessagingTopology::dropIfBlocked)   // отправитель заблокирован → null
    .filter((recipientId, message) -> message != null)          // null'ы отсеиваются
    .processValues(() -> new CensorshipProcessor(BANNED_WORDS_STORE))  // маскирование
    .to(config.filteredMessagesTopic(), Produced.with(...));    // запись результата
```

Устройство `buildBlockedUsersTable`:

```java
builder.stream(blockedUsersTopic, Consumed.with(String, blockCommand))
    .filter((blockerId, command) -> blockerId != null && command != null)  // битые команды до аккумулятора не доходят
    .groupByKey(Grouped.with(String, blockCommand))
    .aggregate(
        HashSet::new,                                  // начальное значение
        MessagingTopology::applyBlockCommand,          // аккумулятор
        Materialized.as(Stores.persistentKeyValueStore(BLOCKED_USERS_STORE))
            .withKeySerde(Serdes.String())
            .withValueSerde(AppSerdes.stringSet()));
```

`persistentKeyValueStore` указан явно, хотя `Materialized.as(String)` и так дал бы
хранилище на диске: задание требует именно persistent state store, и это должно
быть видно в коде.

### BannedWordsUpdater

`ru.practicum.kafka.streams.topology.BannedWordsUpdater` реализует
`Processor<String, String, Void, Void>`. Типы выхода — `Void`: процессор только
пишет в хранилище и ничего не пересылает дальше.

Поля: `storeName` (имя хранилища), `store` (ссылка, получаемая в `init`).

| Метод | Сигнатура | Что делает |
|---|---|---|
| конструктор | `BannedWordsUpdater(String storeName)` | Запоминает имя хранилища; сам store в конструкторе ещё недоступен. |
| `init` | `void init(ProcessorContext<Void, Void> context)` | Получает хранилище: `context.getStateStore(storeName)`. |
| `process` | `void process(Record<String, String> record)` | Применяет одну запись топика `banned_words` к справочнику. |

Логика `process`:

| Условие | Действие | Лог |
|---|---|---|
| `record.key() == null` | запись пропускается | `Запись справочника без ключа пропущена` |
| `record.value() == null` (tombstone) | `store.delete(word)` | `Слово удалено из справочника: {word}` |
| иначе | `store.put(word, record.value())` | `Слово добавлено в справочник: {word}` |

Ключ приводится к нижнему регистру (`record.key().toLowerCase()`): цензор ищет по
нормализованной форме, поэтому «Дурак» из топика и «дурак» в тексте — одно слово.

### CensorshipProcessor

`ru.practicum.kafka.streams.topology.CensorshipProcessor` реализует
`FixedKeyProcessor<String, ChatMessage, ChatMessage>` — вариант процессора,
который на уровне типов запрещает менять ключ записи. Благодаря этому Kafka Streams
знает, что партиционирование сохранилось, и не вставляет repartition-топик.

Поля: `storeName`, `context`, `bannedWords`.

| Метод | Сигнатура | Что делает |
|---|---|---|
| конструктор | `CensorshipProcessor(String storeName)` | Запоминает имя глобального хранилища. |
| `init` | `void init(FixedKeyProcessorContext<String, ChatMessage> context)` | Сохраняет контекст и получает хранилище справочника. |
| `process` | `void process(FixedKeyRecord<String, ChatMessage> record)` | Маскирует запрещённые слова и пересылает запись дальше. |

Логика `process`:

1. `message == null || message.message() == null` → `context.forward(record)` без
   изменений. Битое сообщение уже залогировано в `JsonSerde`, терять факт его
   существования не нужно.
2. `WordMasker.mask(message.message(), word -> bannedWords.get(word) != null)` —
   предикат ходит в хранилище на каждое слово текста.
3. Если текст изменился — лог `Сообщение отцензурировано [{from} -> {to}]: {text}`.
4. `context.forward(record.withValue(message.withMessage(censored)))`.

> **Почему хранилище не подключается явно.** Имя глобального хранилища **не
> передаётся** третьим аргументом в `processValues`. Процессоры получают доступ
> к глобальным хранилищам автоматически, а явное подключение вызвало бы ошибку
> `StateStore ... is not added yet`.

### WordMasker

`ru.practicum.kafka.streams.util.WordMasker` — класс `final` с приватным
конструктором, чистая функция без зависимостей от Kafka.

Константы: `MASK_CHAR = '*'`, `WORD_PATTERN = [\p{L}\p{N}]+` с флагом
`UNICODE_CHARACTER_CLASS`.

| Метод | Сигнатура | Что делает |
|---|---|---|
| `mask` | `public static String mask(String text, Predicate<String> isBanned)` | Заменяет каждое слово, для которого `isBanned` вернул `true`, на звёздочки той же длины. |
| `maskOf` | `private static String maskOf(int length)` | Строка из `length` звёздочек. |

Особенности:

- `null` и пустая строка возвращаются как есть.
- Слово перед проверкой приводится к нижнему регистру — справочник хранится
  нормализованным.
- Пунктуация и пробелы сохраняются, длина текста не меняется.
- Границы слова заданы явным классом символов, а **не** `\b`: в Java `\w` по
  умолчанию — только латиница, цифры и подчёркивание, поэтому на кириллице
  границы срабатывали бы неверно.
- Маскируются только целые слова: «дурака» останется как есть.

### ChatMessage

`ru.practicum.kafka.streams.model.ChatMessage` — **record**, единый формат для
топиков `messages` и `filtered_messages`.

| Поле | JSON | Тип |
|---|---|---|
| `userId` | `user_id` | `String` — отправитель |
| `recipientId` | `recipient_id` | `String` — получатель, он же ключ записи Kafka |
| `message` | `message` | `String` — текст |
| `timestamp` | `timestamp` | `String` — ISO-8601 |

| Метод | Сигнатура | Что делает |
|---|---|---|
| `withMessage` | `public ChatMessage withMessage(String newMessage)` | Копия с заменённым текстом — результат работы цензора. Значение неизменяемо. |

Отправитель называется `user_id`, а не `sender_id`, потому что имена полей заданы
заданием 2 (ksqlDB), и задание 1 использует ту же схему.

### BlockCommand

`ru.practicum.kafka.streams.model.BlockCommand` — **record**, запись топика
`blocked_users`.

| Поле | JSON | Тип |
|---|---|---|
| `blockerId` | `blocker_id` | `String` — кто блокирует, он же ключ записи Kafka |
| `blockedId` | `blocked_id` | `String` — кого блокируют |
| `action` | `action` | `String` — `block` или `unblock` |

Константы: `ACTION_BLOCK = "block"`, `ACTION_UNBLOCK = "unblock"`.

| Метод | Сигнатура | Что делает |
|---|---|---|
| `isBlock` | `public boolean isBlock()` | `true`, если `action` равно `block` без учёта регистра. |

### JsonSerde

`ru.practicum.kafka.streams.serde.JsonSerde<T>` реализует `Serde<T>` — JSON-сериализация
поверх Jackson для любого типа.

Статический `ObjectMapper` настроен с `FAIL_ON_UNKNOWN_PROPERTIES = false`:
неизвестное поле во входящем JSON не роняет обработку.

| Метод | Сигнатура | Что делает |
|---|---|---|
| конструктор | `JsonSerde(TypeReference<T> typeReference)` | Принимает тип через `TypeReference`, а не `Class` — иначе не выразить дженерики вроде `Set<String>`. |
| `serializer` | `Serializer<T> serializer()` | `null` → `null` (tombstone остаётся tombstone'ом, а не строкой `"null"`); ошибка → лог + `null`. |
| `deserializer` | `Deserializer<T> deserializer()` | `null` → `null`; ошибка разбора → лог `Ошибка десериализации из топика {}` + `null`. |

Возврат `null` при ошибке — осознанный контракт: топология отсеивает такие записи
фильтром, приложение продолжает работать.

### AppSerdes

`ru.practicum.kafka.streams.serde.AppSerdes` — класс `final` с приватным
конструктором, фабрика готовых Serde.

| Метод | Сигнатура | Возвращает |
|---|---|---|
| `chatMessage` | `static Serde<ChatMessage> chatMessage()` | Serde сообщений |
| `blockCommand` | `static Serde<BlockCommand> blockCommand()` | Serde команд блокировки |
| `stringSet` | `static Serde<Set<String>> stringSet()` | Serde значения таблицы блокировок |

`stringSet()` существует, потому что готового Serde для коллекций в Kafka Streams
нет. Множество сериализуется как JSON-массив; этим же Serde пользуется
changelog-топик, из которого состояние восстанавливается после перезапуска.

---

## Sequence-диаграммы

### 1. Старт приложения

```mermaid
sequenceDiagram
    autonumber
    participant Docker as docker compose
    participant App as MessagingStreamsApp
    participant Cfg as AppConfig
    participant Topo as MessagingTopology
    participant KS as KafkaStreams
    participant Kafka as Kafka broker

    Docker->>App: main(args)
    App->>Cfg: fromEnvironment()
    Cfg-->>App: AppConfig (топики, brokers, stateDir)

    App->>Topo: build(config)
    Topo->>Topo: buildBlockedUsersTable() → KTable
    Topo->>Topo: addBannedWordsStore() → global store
    Topo->>Topo: leftJoin → filter → processValues → to()
    Topo-->>App: Topology

    App->>App: log.info("Топология: ...")
    App->>Cfg: toStreamsProperties()
    Cfg-->>App: Properties
    App->>KS: new KafkaStreams(topology, props)
    App->>KS: setUncaughtExceptionHandler(REPLACE_THREAD)
    App->>KS: setStateListener(...)
    App->>App: addShutdownHook(streams::close)

    App->>KS: start()
    KS->>Kafka: подписка, восстановление state store
    Note over KS,Kafka: banned_words читается с начала<br/>blocked-users-store — из changelog
    KS-->>App: CREATED → REBALANCING → RUNNING
    App->>App: shutdownLatch.await()
```

### 2. Обновление справочника слов

Отдельный вход в топологию: записи `banned_words` не проходят через основной
конвейер, а только наполняют глобальное хранилище.

```mermaid
sequenceDiagram
    autonumber
    participant Prod as kafka-console-producer
    participant Topic as topic banned_words<br/>(compacted)
    participant BWU as BannedWordsUpdater
    participant Store as banned-words-store<br/>(global, RocksDB)

    Prod->>Topic: key="Дурак", value="1"
    Topic->>BWU: process(Record)
    BWU->>BWU: key == null? нет
    BWU->>BWU: word = key.toLowerCase() → "дурак"
    BWU->>BWU: value == null? нет
    BWU->>Store: put("дурак", "1")
    BWU->>BWU: log.info("Слово добавлено в справочник: дурак")
    Note over Store: доступно всем экземплярам<br/>приложения, без перезапуска
```

### 3. Команда блокировки

```mermaid
sequenceDiagram
    autonumber
    participant Prod as kafka-console-producer
    participant Topic as topic blocked_users
    participant Serde as JsonSerde&lt;BlockCommand&gt;
    participant Topo as MessagingTopology
    participant Store as blocked-users-store<br/>(RocksDB + changelog)

    Prod->>Topic: key="bob"<br/>{"blocker_id":"bob","blocked_id":"eve","action":"block"}
    Topic->>Serde: deserialize(bytes)
    Serde-->>Topo: BlockCommand(bob, eve, block)

    Topo->>Topo: filter(blockerId != null && command != null)
    Topo->>Topo: groupByKey() → по blocker_id
    Topo->>Topo: aggregate(HashSet::new, applyBlockCommand)

    Topo->>Topo: applyBlockCommand("bob", cmd, {})
    Topo->>Topo: command.isBlock() → true
    Topo->>Topo: blocked.add("eve")
    Topo->>Topo: log.info("bob заблокировал eve")

    Topo->>Store: put("bob", {"eve"})
    Store->>Store: запись в changelog-топик
    Note over Store: KTable: одна запись на пользователя<br/>значение — множество заблокированных
```

Команда `unblock` идёт тем же путём, только `applyBlockCommand` вызывает
`blocked.remove(...)` и пишет в лог `bob разблокировал eve`.

### 4. Главный поток: обработка сообщения

Сообщение доходит и цензурируется — путь alice → carol с текстом «ты дурак, шучу :)».

```mermaid
sequenceDiagram
    autonumber
    participant Topic as topic messages
    participant Serde as JsonSerde&lt;ChatMessage&gt;
    participant Join as leftJoin
    participant BStore as blocked-users-store
    participant Filter as filter
    participant CP as CensorshipProcessor
    participant WM as WordMasker
    participant WStore as banned-words-store
    participant Out as topic filtered_messages

    Topic->>Serde: key="carol", value=JSON
    Serde-->>Join: ChatMessage(alice → carol, "ты дурак, шучу :)")

    Join->>BStore: get("carol")
    BStore-->>Join: null (carol никого не блокировала)
    Join->>Join: dropIfBlocked(message, null)
    Note over Join: blockedByRecipient == null<br/>→ сообщение возвращается как есть
    Join-->>Filter: ChatMessage

    Filter->>Filter: message != null → пропустить
    Filter-->>CP: process(FixedKeyRecord)

    CP->>CP: message.message() != null
    CP->>WM: mask("ты дурак, шучу :)", isBanned)

    loop для каждого слова текста
        WM->>WStore: get(word.toLowerCase())
        WStore-->>WM: "1" для "дурак", null для остальных
    end

    WM-->>CP: "ты *****, шучу :)"
    CP->>CP: текст изменился → log.info("Сообщение отцензурировано")
    CP->>Out: forward(record.withValue(message.withMessage(censored)))
    Note over Out: ключ не изменился (FixedKeyProcessor)<br/>→ repartition не нужен
```

### 5. Сообщение от заблокированного отправителя

Путь eve → bob: bob ранее заблокировал eve.

```mermaid
sequenceDiagram
    autonumber
    participant Topic as topic messages
    participant Serde as JsonSerde&lt;ChatMessage&gt;
    participant Join as leftJoin
    participant BStore as blocked-users-store
    participant Filter as filter
    participant CP as CensorshipProcessor
    participant Out as topic filtered_messages

    Topic->>Serde: key="bob", value=JSON
    Serde-->>Join: ChatMessage(eve → bob, "это сообщение не дойдёт")

    Join->>BStore: get("bob")
    BStore-->>Join: {"eve"}
    Join->>Join: dropIfBlocked(message, {"eve"})
    Join->>Join: blockedByRecipient.contains("eve") → true
    Join->>Join: log.info("Сообщение отброшено: eve заблокирован у bob")
    Join-->>Filter: null

    Filter->>Filter: message != null → false
    Note over Filter: запись отсеяна
    Filter--xCP: не вызывается
    Filter--xOut: ничего не записано
```

Ключ сообщения — **получатель**, ключ таблицы — **блокирующий**: это один и тот же
пользователь, поэтому входы co-partitioned и соединение выполняется локально.

### 6. Удаление слова через tombstone

```mermaid
sequenceDiagram
    autonumber
    participant Prod as kafka-console-producer
    participant Topic as topic banned_words<br/>(compacted)
    participant BWU as BannedWordsUpdater
    participant Store as banned-words-store
    participant CP as CensorshipProcessor

    Prod->>Topic: key="дурак", value=null<br/>(--property parse.null=true)
    Topic->>BWU: process(Record)
    BWU->>BWU: key != null, word = "дурак"
    BWU->>BWU: record.value() == null → tombstone
    BWU->>Store: delete("дурак")
    BWU->>BWU: log.info("Слово удалено из справочника: дурак")

    Note over Prod,CP: следующее сообщение с этим словом

    CP->>Store: get("дурак")
    Store-->>CP: null
    Note over CP: слово больше не маскируется,<br/>приложение не перезапускалось
```

Compaction гарантирует, что при следующем чтении топика с начала удалённое слово
не вернётся: tombstone вытесняет предыдущее значение того же ключа.

### 7. Битый JSON

Требование задания — записать ошибку в лог и продолжить работу.

```mermaid
sequenceDiagram
    autonumber
    participant Topic as topic messages
    participant Serde as JsonSerde&lt;ChatMessage&gt;
    participant Join as leftJoin
    participant Filter as filter
    participant Out as topic filtered_messages

    Topic->>Serde: value = "{это не json"
    Serde->>Serde: MAPPER.readValue() → Exception
    Serde->>Serde: log.error("Ошибка десериализации из топика messages: ...")
    Serde-->>Join: null

    Join->>Join: dropIfBlocked(null, ...)
    Note over Join: message == null → возвращается null
    Join-->>Filter: null
    Filter->>Filter: message != null → false
    Filter--xOut: запись отсеяна

    Note over Topic,Out: обработка следующих записей продолжается,<br/>приложение не падает
```

Битая команда блокировки отсеивается раньше — фильтром
`blockerId != null && command != null` до аккумулятора, чтобы не испортить состояние.

### 8. Перезапуск и восстановление состояния

```mermaid
sequenceDiagram
    autonumber
    participant Docker as docker compose restart
    participant App as MessagingStreamsApp
    participant Hook as shutdown hook
    participant KS as KafkaStreams
    participant Vol as volume streams-state<br/>(RocksDB)
    participant Kafka as Kafka broker

    Docker->>App: SIGTERM
    App->>Hook: run()
    Hook->>Hook: log.info("Получен сигнал завершения...")
    Hook->>KS: close()
    KS->>Kafka: досылка буферов, commit offsets, выход из группы
    Hook->>App: shutdownLatch.countDown()
    App->>App: log.info("Приложение остановлено.")

    Note over Docker,Kafka: контейнер стартует заново

    Docker->>App: main(args)
    App->>KS: start()
    KS->>Vol: чтение blocked-users-store с диска
    Vol-->>KS: {"bob" → {"eve"}}
    KS->>Kafka: дочитывание changelog с последнего offset
    KS->>Kafka: чтение banned_words с начала (compacted)
    Kafka-->>KS: дурак, идиот
    KS-->>App: RUNNING

    Note over App,Kafka: блокировки и справочник действуют<br/>без повторной заливки
```

---

## Привязка требований задания к коду

### Задание 1

| # | Требование | Где реализовано |
|---|---|---|
| 1 | Библиотека потоковой обработки | `src/build.gradle.kts` → `org.apache.kafka:kafka-streams:3.7.0` |
| 2 | Отдельный список заблокированных на пользователя | `MessagingTopology.buildBlockedUsersTable` → `KTable<String, Set<String>>` |
| 3 | Список в persistent state store | там же → `Materialized.as(Stores.persistentKeyValueStore(BLOCKED_USERS_STORE))` |
| 4 | Сообщения от заблокированных не доходят | `MessagingTopology.dropIfBlocked` + `.filter(...)` → [диаграмма 5](#5-сообщение-от-заблокированного-отправителя) |
| 5 | Динамическое обновление списка слов | `BannedWordsUpdater.process` → [диаграммы 2](#2-обновление-справочника-слов) и [6](#6-удаление-слова-через-tombstone) |
| 6 | Компонент, маскирующий слова | `CensorshipProcessor` + `WordMasker.mask` |
| 7 | Все сообщения проходят цензуру | `.processValues(...)` стоит на пути каждого сообщения к `.to(filteredMessagesTopic)` |
| 8 | Docker-compose разворачивает систему | `docker-compose.yml`, профили `apps` / `ksqldb` / `tools` |
| 9 | Топики `messages`, `filtered_messages`, `blocked_users` | `AppConfig` + шаг 2 в [QUICKSTART.md](QUICKSTART.md#шаг-2-создать-топики) |
| 10 | Тестовые данные | `test-data/` — три файла |
| 11 | Логичная структура и комментарии | разделение на `config` / `model` / `serde` / `topology` / `util` |
| 12 | Состояние переживает перезапуск | volume `streams-state` + changelog → [диаграмма 8](#8-перезапуск-и-восстановление-состояния) |

### Задание 2

| # | Требование | Где реализовано |
|---|---|---|
| 1 | Поток `messages_stream` | `ksqldb-queries.sql`, запрос 1 |
| 2 | Общее количество сообщений | таблица `total_messages` → `5` |
| 3 | Уникальные получатели | таблица `total_unique_recipients` → `3` |
| 4–6 | Статистика по пользователям | таблица `user_statistics`: `COUNT(*)` и `COUNT_DISTINCT(recipient_id)` в одном `GROUP BY` |

---

## Состояние: где что лежит

| Store | Содержимое | Тип | Наполняется | Читается |
|---|---|---|---|---|
| `blocked-users-store` | `user → множество заблокированных им` | persistent (RocksDB), с changelog-топиком | `aggregate` из потока `blocked_users` | `leftJoin` |
| `banned-words-store` | `слово → маркер` | persistent, **глобальный** (полная копия на каждом экземпляре) | `BannedWordsUpdater` из топика `banned_words` | `CensorshipProcessor` напрямую |

Физически оба лежат в каталоге из `KAFKA_STATE_DIR` (в контейнере —
`/var/lib/streams-state`, примонтированный volume `streams-state`).

Восстановление после перезапуска различается: `blocked-users-store` дочитывается
из changelog-топика, который Kafka Streams создаёт сам; `banned-words-store`
перечитывается из исходного compacted-топика `banned_words` с начала.

---

## Тесты

Топология проверяется через `TopologyTestDriver` — брокер не нужен, записи
подаются на вход и читаются с выхода в памяти.

**`MessagingTopologyTest`** (6 тестов):

| Тест | Что проверяет |
|---|---|
| `dropsMessageFromBlockedSender` | Сообщение от заблокированного отправителя не доходит до получателя |
| `deliversAfterUnblock` | Разблокировка возвращает доставку сообщений |
| `masksBannedWord` | Запрещённое слово маскируется в тексте |
| `stopsMaskingAfterTombstone` | Удаление слова из справочника отменяет цензуру без перезапуска |
| `appliesBothStages` | Блокировка и цензура применяются вместе, в одном проходе |
| `keepsBlockedUsersInStateStore` | Список блокировок лежит в state store и переживает обработку |

**`WordMaskerTest`** (5 тестов):

| Тест | Что проверяет |
|---|---|
| `masksBannedWord` | Слово заменяется звёздочками той же длины |
| `masksIgnoringCase` | Регистр не важен — справочник хранится в нижнем регистре |
| `keepsCleanTextAsIs` | Текст без запрещённых слов не меняется |
| `doesNotMaskSubstring` | Часть слова не маскируется, граница слова учитывается |
| `handlesEmptyInput` | Пустой текст и `null` не ломают маскирование |

Запуск — [QUICKSTART.md, шаг 7.5](QUICKSTART.md#75-автотесты-без-брокера).
