# Практическая работа 3. Блокировка и цензура сообщений на Streamiz (.NET)

Сервис обмена сообщениями: пользователи блокируют нежелательных отправителей,
запрещённые слова маскируются. Списки блокировок и слов живут в persistent state
store и обновляются на лету, без перезапуска приложения. Реализация — .NET 8,
[Streamiz.Kafka.Net](https://github.com/LGouellec/streamiz) 1.8.1.

Та же топология на Java (эталонная Kafka Streams) — в ветке
[`practicum-java`](../../tree/practicum-java/practicum-java).
Термины (KStream, KTable, state store, changelog, tombstone) — в
[глоссарии модуля](../../blob/main/glossary.md).

## Содержание

- [Почему C#](#почему-c)
- [Архитектура](#архитектура)
- [Запуск](#запуск)
- [Проверка работы](#проверка-работы)
- [Классы и параметры](#классы-и-параметры)
- [Задание 2. Аналитика через ksqlDB](#задание-2-аналитика-через-ksqldb)
- [Проверка соответствия заданию](#проверка-соответствия-заданию)
- [Как это работает](#как-это-работает)
- [Обработка ошибок](#обработка-ошибок)
- [Ограничения и компромиссы](#ограничения-и-компромиссы)

## Почему C#

Задание предлагает Java, Python или Go. Работа сделана на **C# / .NET 8** с
библиотекой [Streamiz.Kafka.Net](https://github.com/LGouellec/streamiz) — портом
Kafka Streams на .NET (MIT). Это не самодельная имитация: библиотека даёт те же
примитивы, включая persistent state store поверх RocksDB — того же движка, что
использует Kafka Streams в Java.

| Примитив | Java (`org.apache.kafka.streams`) | C# (`Streamiz.Kafka.Net`) |
|---|---|---|
| Сборка топологии | `StreamsBuilder` | `StreamBuilder` |
| Поток / таблица | `builder.stream(...)` / `builder.table(...)` | `builder.Stream<K,V>(...)` / `builder.Table<K,V>(...)` |
| Агрегация в state store | `groupByKey().aggregate(..., Materialized.as(Stores.persistentKeyValueStore(...)))` | `GroupByKey().Aggregate(..., RocksDb.As<K,V>("store"))` |
| Глобальная таблица | `builder.globalTable(...)` / `addGlobalStore(...)` | `builder.GlobalTable(...)` / `AddGlobalStore(...)` |
| Доступ к state store | `context.getStateStore(name)` | `context.GetStateStore(name)` |
| Processor API | `processValues(FixedKeyProcessorSupplier)` | `TransformValues(TransformerBuilder...)` |
| Сериализация | `Serde<T>` | `ISerDes<T>` |
| Движок persistent store | RocksDB | RocksDB (та же нативная библиотека) |
| Запуск | `new KafkaStreams(topology, props).start()` | `new KafkaStream(topology, config).StartAsync()` |
| Тесты без брокера | `TopologyTestDriver` | `TopologyTestDriver` |

Различия только в именовании API и в зрелости экосистемы: Streamiz — community-проект,
Kafka Streams — эталонная реализация Apache. Задача решается теми же средствами.

**Версия .NET — 8, а не 10** как в первом модуле: Streamiz 1.8.1 заявляет поддержку
по .NET 8 включительно, и брать непроверенную комбинацию ради единообразия версий
смысла нет.

## Архитектура

```
   blocked_users                      messages                    banned_words
 (команды block/unblock)         (входящие сообщения)          (compacted, слово=ключ)
         │                                │                              │
         │ GroupByKey + Aggregate         │                              │ GlobalTable
         ▼                                │                              ▼
 ┌───────────────────┐                    ▼                    ┌──────────────────┐
 │ blocked-users-    │           TransformValues               │ banned-words-    │
 │ store (RocksDB)   │──читает──▶ 1. блокировка ◀────читает────│ store (RocksDB,  │
 │ user → {кого он   │            2. цензура                   │ глобальный)      │
 │ заблокировал}     │                    │                    └──────────────────┘
 └───────────────────┘                    ▼
                                   Filter (≠ null)
                                          │
                                          ▼
                                  filtered_messages
```

Обе стадии — **один граф в одном процессе**. Промежуточного топика между блокировкой
и цензурой нет: это внутренний шаг топологии.

```
practicum-dotnet/
├── docker-compose.yml       Kafka (KRaft) + приложение + ksqlDB + Kafka UI
├── ksqldb-queries.sql       задание 2: запросы аналитики
├── test-data/               тестовые сообщения, блокировки и слова
└── src/
    ├── practicum-dotnet.slnx
    ├── Practicum.Kafka.Streams/
    │   ├── Program.cs                     точка входа, Serilog, graceful shutdown
    │   ├── Configuration/AppConfig.cs     настройки из переменных окружения
    │   ├── Models/                        ChatMessage, BlockCommand
    │   ├── Serialization/                 JSON-SerDes, включая ISerDes<HashSet<string>>
    │   ├── Topology/                      MessagingTopology + MessageFilterTransformer
    │   ├── WordMasker.cs                  маскирование слов
    │   └── Dockerfile
    └── Practicum.Kafka.Streams.Tests/     тесты топологии и маскирования
```

## Запуск

Нужны Docker и Docker Compose v2. .NET 8 SDK понадобится, только если запускать
приложение без Docker.

Порядок шагов важен: топики создаются **вручную**, автосоздание на брокере отключено.
Приложение вынесено в профиль `apps` и поэтому не стартует раньше времени.

### 1. Поднять брокер

```bash
cd practicum-dotnet
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
docker exec -i practicum3-kafka kafka-console-producer \
  --topic blocked_users --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/blocked_users.jsonl

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

### 5. Отправить сообщения и посмотреть результат

```bash
docker exec -i practicum3-kafka kafka-console-producer \
  --topic messages --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/messages.jsonl

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

### Блокировка в динамике

```bash
# carol блокирует alice
echo 'carol:{"blocker_id":"carol","blocked_id":"alice","action":"block"}' | \
  docker exec -i practicum3-kafka kafka-console-producer \
    --topic blocked_users --bootstrap-server kafka:29092 \
    --property parse.key=true --property key.separator=:

# Следующее сообщение alice → carol уже не дойдёт.
# Команда с "action":"unblock" возвращает доставку.
```

### Динамическое обновление списка слов

Список меняется без перезапуска приложения. Добавить слово:

```bash
echo "привет:1" | docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=:
```

Убрать слово — записью с пустым значением (tombstone):

```bash
echo "привет:" | docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  --property null.marker= --property parse.null=true
```

После любой из этих команд повторно отправьте сообщение с этим словом и сравните
результат в `filtered_messages`.

### Персистентность состояния

```bash
docker compose --profile apps restart messaging-streams-app
```

После перезапуска блокировки и список слов действуют **без повторной заливки**:
состояние лежит на диске в томе и восстанавливается из changelog-топика.

### Автотесты

Топология проверяется без брокера через `TopologyTestDriver`:

```bash
cd src/Practicum.Kafka.Streams.Tests
dotnet test
```

11 тестов: маскирование слов (5) и топология целиком (6) — блокировка,
разблокировка, цензура, tombstone, обе стадии вместе, содержимое state store.
Все проходят.

## Классы и параметры

| Класс | Назначение |
|---|---|
| `Program` | Точка входа: настраивает Serilog, собирает топологию, запускает `KafkaStream`, обрабатывает сигнал завершения. |
| `MessagingTopology` | Весь граф обработки: таблица блокировок, глобальный справочник слов, join, фильтр, цензура. |
| `MessageFilterTransformer` | `ITransformer`: обе стадии — проверяет блокировку и маскирует слова, читая оба хранилища напрямую. Ключ не меняет. |
| `WordMasker` | Чистая функция маскирования. Вынесена отдельно, чтобы проверяться юнит-тестом. |
| `ChatMessage`, `BlockCommand` | Модели сообщения и команды блокировки. |
| `JsonSerDes<T>`, `AppSerDes` | JSON-сериализация; ошибка разбора логируется и даёт `default`. |
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
| `KAFKA_STATE_DIR` | временный каталог | каталог persistent state store |
| `KAFKA_TOPIC_MESSAGES` | `messages` | входящие сообщения |
| `KAFKA_TOPIC_FILTERED_MESSAGES` | `filtered_messages` | сообщения после обработки |
| `KAFKA_TOPIC_BLOCKED_USERS` | `blocked_users` | команды блокировки |
| `KAFKA_TOPIC_BANNED_WORDS` | `banned_words` | справочник запрещённых слов |
| `LOG_LEVEL` | `Information` | уровень логирования Serilog |

## Задание 2. Аналитика через ksqlDB

Дополнительное задание, поднимается отдельным профилем. Часть от языка приложения
не зависит — запросы те же, что в Java-ветке.

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

## Проверка соответствия заданию

### Задание 1

| # | Требование | Как проверить | Ожидаемый результат |
|---|---|---|---|
| 1 | Библиотека потоковой обработки | `Practicum.Kafka.Streams.csproj` | зависимость `Streamiz.Kafka.Net` — порт Kafka Streams |
| 2 | Отдельный список заблокированных для каждого пользователя | `MessagingTopology.BuildBlockedUsersTable` | `IKTable<user, HashSet<string>>` — одна запись на пользователя |
| 3 | Список хранится в persistent state store | там же | `RocksDb.As<string, HashSet<string>>("blocked-users-store")` |
| 4 | Сообщения от заблокированных не доходят | шаги 3–5 запуска | сообщения от `eve` нет в `filtered_messages`, в логе `Сообщение отброшено` |
| 5 | Список запрещённых слов обновляется динамически | [раздел выше](#динамическое-обновление-списка-слов) | новое слово маскируется без перезапуска; tombstone отменяет маскирование |
| 6 | Компонент, удаляющий или маскирующий слова | `MessageFilterTransformer` + `WordMasker` | «дурак» → `*****` |
| 7 | Все сообщения проходят цензуру | `MessagingTopology.Build` | `TransformValues` стоит на пути каждого сообщения к `filtered_messages` |
| 8 | Docker-compose разворачивает систему | `docker compose up -d` | брокер `healthy`, приложение `Up` |
| 9 | Топики `messages`, `filtered_messages`, `blocked_users` | шаг 2 запуска | `kafka-topics --list` показывает все три (плюс `banned_words`) |
| 10 | Тестовые данные | `test-data/` | три файла, команды заливки в шагах 3 и 5 |
| 11 | Логичная структура и понятные комментарии | `src/Practicum.Kafka.Streams/` | разделение на модель, сериализацию, топологию, утилиты |
| 12 | Состояние переживает перезапуск | [раздел выше](#персистентность-состояния) | после `restart` блокировки действуют без повторной заливки |

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
для каждого сообщения нужно достать список блокировок его получателя.

Хранилище блокировок разделено между экземплярами приложения по партициям, поэтому
нужная запись обязана лежать в той же партиции, что и обрабатываемое сообщение.
Ключ записи в `messages` — `recipient_id`, ключ хранилища — `blocker_id`, и это один
и тот же пользователь: условие выполняется само собой, repartition-топик не нужен.

### Почему не join

Естественным решением выглядел бы `LeftJoin` потока с таблицей блокировок. На этой
версии Streamiz он на реальном брокере не отдаёт значение таблицы: объединителю
всегда приходит `null`, хотя запись в хранилище есть и партиционирование корректное
(в тестах через `TopologyTestDriver` тот же join при этом работает).

Поэтому обе проверки делает один `ITransformer`, читающий оба хранилища напрямую
через `GetStateStore`. Для справочника слов такой доступ нужен в любом случае
(см. ниже), так что решение единообразно: блокировка — один точечный lookup,
цензура — по одному на каждое слово текста.

Внешнее поведение от этого не меняется: топология остаётся одним графом в одном
процессе, партиционирование сохраняется (`TransformValues` не даёт менять ключ),
промежуточных топиков не появляется.

### Почему команды, а не готовый список

В топик `blocked_users` кладутся команды `block`/`unblock`, а не полный список
блокировок пользователя. Иначе клиенту пришлось бы знать текущий список целиком
перед каждым изменением, а параллельные блокировки от разных клиентов затирали бы
друг друга.

Топология сворачивает поток команд в множество (`GroupByKey().Aggregate(...)`) —
это и есть тот самый «отдельный список заблокированных для каждого пользователя»
из задания, лежащий в persistent state store.

### Почему справочник слов — глобальная таблица

Запрещённые слова не связаны с ключом сообщения: для маскирования нужно проверить
**каждое слово текста**, а не одно значение по ключу записи. Обычная таблица здесь
не подходит — она делится между экземплярами приложения по партициям, и нужного
слова могло бы просто не оказаться на этом экземпляре.

`GlobalTable` даёт каждому экземпляру полную копию справочника. Цензор обращается
к её хранилищу напрямую (`GetStateStore`), потому что на одно сообщение нужно
столько обращений, сколько в нём слов, — а join сопоставляет запись потока ровно
с одной записью таблицы.

### Почему compacted-топик и tombstone

Справочник слов — это состояние, а не поток событий: важно текущее содержимое списка,
а не история его изменений. Compacted-топик хранит последнее значение каждого ключа,
поэтому список полностью восстанавливается чтением топика с начала.

Удаление слова — запись со значением `null` (tombstone). Альтернативой была бы
команда вида `{"word":"...","action":"remove"}`, но тогда пришлось бы вручную писать
логику вычитания, а топик рос бы вечно.

### Кодировка кириллицы

`System.Text.Json` по умолчанию экранирует всё, кроме базовой латиницы, и кириллица
уходила бы в топик нечитаемыми escape-последовательностями. Поэтому в `JsonSerDes`
задан `Encoder` с разрешённым диапазоном кириллицы — в Kafka UI и в выводе
`kafka-console-consumer` текст виден как есть.

### Порядок стадий

Сначала блокировка, потом цензура: проверка вхождения в множество дешевле разбора
текста, и нет смысла цензурировать сообщение, которое всё равно будет отброшено.

## Обработка ошибок

| Ситуация | Поведение |
|---|---|
| Битый JSON во входящем сообщении | `JsonSerDes` пишет ошибку в лог и возвращает `default`; запись отсеивается фильтром, обработка продолжается |
| Битая команда блокировки | то же: отсеивается до аккумулятора, состояние не портится |
| Сообщение без текста | проходит дальше без цензуры, не роняя топологию |
| Недоступен брокер | клиент Kafka переподключается сам; при старте приложение ждёт брокера |
| Непредвиденная ошибка при старте | пишется в лог с уровнем Critical, приложение завершается с кодом 1 |
| `docker stop` | при остановке досылаются буферы, коммитятся смещения, приложение выходит из группы |

В продакшене неразбираемые сообщения уходили бы в отдельный топик (Dead Letter Queue),
здесь по заданию достаточно записи в лог.

## Ограничения и компромиссы

**Топик `banned_words` добавлен сверх списка задания.** Задание перечисляет `messages`,
`filtered_messages` и `blocked_users`, но требует «динамически обновляемый список
запрещённых слов». Список должен где-то храниться и обновляться без перезапуска —
топик единственный способ это сделать в рамках Kafka.

**Один брокер вместо кластера.** В первом модуле поднимались три брокера с репликацией.
Здесь фокус на потоковой обработке, а не на кластерных гарантиях, поэтому брокер один
и в режиме KRaft (без ZooKeeper). Следствие: `replication.factor=1` у всех топиков.

**Маскирование вместо удаления.** Задание допускает и то, и другое. Выбрано
маскирование звёздочками: получателю видно, что слово было вырезано, а длина текста
не меняется.

**Цензура работает по отдельным словам.** Запрещённое слово внутри другого слова
не маскируется («дурака» останется как есть). Это осознанно: поиск по подстроке
даёт ложные срабатывания на безобидных словах.

**Логгер процессоров передаётся через статическое свойство.** Streamiz создаёт
процессоры сам, конструктором без параметров, поэтому внедрить зависимость обычным
способом некуда.

**Что можно улучшить.** Schema Registry для схем данных, Dead Letter Queue для
неразбираемых сообщений, exactly-once семантика, Interactive Queries для просмотра
состояния по HTTP.
