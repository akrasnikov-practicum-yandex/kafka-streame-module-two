# Пошаговый запуск с нуля

Инструкция для первого запуска: от чистой машины до проверенного результата.
Команды даны в двух вариантах — **bash** (Linux, macOS, Git Bash, WSL) и
**PowerShell 7** (Windows). Выберите один вариант и держитесь его до конца:
смешивать нельзя, различается синтаксис перенаправления и кавычек.

Архитектура, разбор решений и соответствие заданию — в [README.md](README.md).

## Содержание

- [Шаг 0. Проверка окружения](#шаг-0-проверка-окружения)
- [Шаг 1. Поднять брокер](#шаг-1-поднять-брокер)
- [Шаг 2. Создать топики](#шаг-2-создать-топики)
- [Шаг 3. Загрузить справочники](#шаг-3-загрузить-справочники)
- [Шаг 4. Запустить приложение](#шаг-4-запустить-приложение)
- [Шаг 5. Отправить сообщения](#шаг-5-отправить-сообщения)
- [Шаг 6. Проверить результат](#шаг-6-проверить-результат)
- [Шаг 7. Проверить динамику](#шаг-7-проверить-динамику)
- [Шаг 8. Аналитика ksqlDB (задание 2)](#шаг-8-аналитика-ksqldb-задание-2)
- [Остановка и очистка](#остановка-и-очистка)
- [Если что-то пошло не так](#если-что-то-пошло-не-так)

---

## Шаг 0. Проверка окружения

Нужны только Docker и Docker Compose v2. JDK 21 понадобится, лишь если запускать
приложение без Docker или гонять тесты локальным Gradle.

```bash
docker --version           # ожидается 24.0 или новее
docker compose version     # ожидается v2.x
git --version
```

Всё то же самое в PowerShell — команды одинаковые.

Проверьте, что порты `9092` (брокер), `8080` (Kafka UI) и `8088` (ksqlDB)
свободны:

```bash
# bash
ss -ltn | grep -E '9092|8080|8088' || echo "порты свободны"
```

```powershell
# PowerShell
Get-NetTCPConnection -State Listen -LocalPort 9092,8080,8088 -ErrorAction SilentlyContinue
```

Пустой вывод = порты свободны.

### Получить код

Реализация лежит в ветке `practicum-java`, в `main` только индекс модуля.

```bash
git clone https://github.com/akrasnikov-practicum-yandex/kafka-streame-module-two.git
cd kafka-streame-module-two
git checkout practicum-java
cd practicum-java
```

Дальше все команды выполняются **из каталога `practicum-java/`** — там лежит
`docker-compose.yml`, а пути к тестовым данным указаны относительно него.

```bash
ls                         # docker-compose.yml, ksqldb-queries.sql, src/, test-data/
```

---

## Шаг 1. Поднять брокер

Без профиля поднимается только Kafka: приложение вынесено в профиль `apps` и не
стартует раньше времени — топики создаются вручную на шаге 2, а автосоздание на
брокере отключено.

```bash
docker compose up -d
```

Дождитесь статуса `healthy` — на это уходит 20–40 секунд:

```bash
docker compose ps
```

Ожидаемый вывод:

```
NAME               IMAGE                          STATUS
practicum3-kafka   confluentinc/cp-kafka:7.6.0    Up 35 seconds (healthy)
```

Пока в колонке `STATUS` написано `(health: starting)` — ждите. Следующий шаг
на неготовом брокере упадёт с `Connection refused`.

Дождаться автоматически:

```bash
# bash
until [ "$(docker inspect -f '{{.State.Health.Status}}' practicum3-kafka)" = "healthy" ]; do sleep 2; done; echo "брокер готов"
```

```powershell
# PowerShell
while ((docker inspect -f '{{.State.Health.Status}}' practicum3-kafka) -ne 'healthy') { Start-Sleep 2 }; 'брокер готов'
```

---

## Шаг 2. Создать топики

Четыре топика. Три первых обычные, `banned_words` — **compacted**: Kafka хранит
последнее значение каждого ключа, поэтому актуальный список слов восстанавливается
чтением топика с начала, а запись со значением `null` (tombstone) убирает слово
насовсем. Созданный автоматически топик получил бы обычную политику удаления,
и справочник терялся бы по retention — поэтому автосоздание и выключено.

```bash
# bash
docker exec -i practicum3-kafka bash -c '
  for t in messages filtered_messages blocked_users; do
    kafka-topics --create --topic $t --bootstrap-server kafka:29092 \
      --partitions 3 --replication-factor 1
  done

  kafka-topics --create --topic banned_words --bootstrap-server kafka:29092 \
    --partitions 3 --replication-factor 1 --config cleanup.policy=compact
'
```

```powershell
# PowerShell — цикл выполняется в контейнере, поэтому команда одна и та же,
# но без обратных слэшей: PowerShell трактует их иначе.
docker exec -i practicum3-kafka bash -c 'for t in messages filtered_messages blocked_users; do kafka-topics --create --topic $t --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1; done; kafka-topics --create --topic banned_words --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1 --config cleanup.policy=compact'
```

Ожидаемый вывод — четыре строки:

```
Created topic messages.
Created topic filtered_messages.
Created topic blocked_users.
Created topic banned_words.
```

Проверка:

```bash
docker exec -it practicum3-kafka kafka-topics --list --bootstrap-server kafka:29092
```

```
banned_words
blocked_users
filtered_messages
messages
```

Убедиться, что `banned_words` действительно compacted:

```bash
docker exec -it practicum3-kafka kafka-topics --describe --topic banned_words --bootstrap-server kafka:29092
```

В строке `Configs:` должно быть `cleanup.policy=compact`. Если там `delete` —
топик создан неверно, удалите (`kafka-topics --delete --topic banned_words
--bootstrap-server kafka:29092`) и повторите шаг.

> **Адреса брокера.** Изнутри docker-сети — `kafka:29092`, с хост-машины —
> `localhost:9092`. Все команды ниже выполняются внутри контейнера, поэтому
> везде `kafka:29092`.

---

## Шаг 3. Загрузить справочники

Блокировки и запрещённые слова заливаются **до** сообщений, чтобы состояние успело
собраться к моменту обработки. Формат строк в файлах — `ключ:значение`,
разделитель задаётся `--property key.separator=:`.

Содержимое файлов:

- `test-data/blocked_users.jsonl` — одна команда: `bob` блокирует `eve`;
- `test-data/banned_words.txt` — два слова: `дурак`, `идиот`.

```bash
# bash — блокировки
docker exec -i practicum3-kafka kafka-console-producer \
  --topic blocked_users --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/blocked_users.jsonl

# bash — запрещённые слова
docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/banned_words.txt
```

```powershell
# PowerShell — блокировки
Get-Content test-data/blocked_users.jsonl -Raw -Encoding utf8 | docker exec -i practicum3-kafka kafka-console-producer --topic blocked_users --bootstrap-server kafka:29092 --property parse.key=true --property key.separator=:

# PowerShell — запрещённые слова
Get-Content test-data/banned_words.txt -Raw -Encoding utf8 | docker exec -i practicum3-kafka kafka-console-producer --topic banned_words --bootstrap-server kafka:29092 --property parse.key=true --property key.separator=:
```

> **Почему в PowerShell не `<`.** Оператор `<` в PowerShell зарезервирован и не
> перенаправляет файл в stdin, а `cmd`-подобный вызов перекодировал бы содержимое
> в UTF-16 — кириллица в топике превратилась бы в мусор. `Get-Content -Raw
> -Encoding utf8` отдаёт байты как есть.

Обе команды завершаются молча, без вывода — это нормально. Проверка, что данные
легли (`--timeout-ms` нужен, чтобы консьюмер не висел в ожидании новых записей):

```bash
docker exec -it practicum3-kafka kafka-console-consumer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --from-beginning --property print.key=true --timeout-ms 5000
```

```
дурак	1
идиот	1
```

Строка `Processed a total of 2 messages` в конце и сообщение о `TimeoutException`
— это штатное завершение по `--timeout-ms`, не ошибка.

---

## Шаг 4. Запустить приложение

Первый запуск собирает образ (многоступенчатая сборка Gradle + JRE) — это
занимает несколько минут, дальше образ кэшируется.

```bash
docker compose --profile apps up -d --build
docker compose logs -f messaging-streams-app
```

В логе ожидаются:

```
Слово добавлено в справочник: дурак
Слово добавлено в справочник: идиот
bob заблокировал eve
Состояние приложения: CREATED -> REBALANCING
Состояние приложения: REBALANCING -> RUNNING
Приложение запущено. Брокеры=kafka:29092, application.id=practicum3-messaging-streams.
```

Дождитесь строки `REBALANCING -> RUNNING` — до неё приложение сообщения не
обрабатывает. Выйти из просмотра логов — `Ctrl+C` (контейнер продолжит работать).

Проверка состояния:

```bash
docker compose --profile apps ps
```

`practicum3-streams-app` должен быть `Up`. Если он в `Restarting` — смотрите
[раздел про ошибки](#приложение-перезапускается-по-кругу).

---

## Шаг 5. Отправить сообщения

Пять сообщений. Ключ каждого — **получатель** (`recipient_id`): именно по нему
поток соединяется с таблицей блокировок, и связанные записи обязаны лежать в одной
партиции.

```bash
# bash
docker exec -i practicum3-kafka kafka-console-producer \
  --topic messages --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  < test-data/messages.jsonl
```

```powershell
# PowerShell
Get-Content test-data/messages.jsonl -Raw -Encoding utf8 | docker exec -i practicum3-kafka kafka-console-producer --topic messages --bootstrap-server kafka:29092 --property parse.key=true --property key.separator=:
```

В логе приложения сразу появятся следы обработки:

```
Сообщение отброшено: eve заблокирован у bob
Сообщение отцензурировано [alice -> carol]: ты ***** шучу :)
```

---

## Шаг 6. Проверить результат

```bash
docker exec -it practicum3-kafka kafka-console-consumer \
  --topic filtered_messages --bootstrap-server kafka:29092 \
  --from-beginning --property print.key=true --timeout-ms 10000
```

Из пяти отправленных сообщений приходят **четыре**:

```
bob	{"user_id":"alice","recipient_id":"bob","message":"привет, как дела?",...}
carol	{"user_id":"alice","recipient_id":"carol","message":"ты ***** шучу :)",...}
alice	{"user_id":"bob","recipient_id":"alice","message":"всё отлично, спасибо!",...}
alice	{"user_id":"carol","recipient_id":"alice","message":"привет от carol",...}
```

Что произошло с каждым сообщением:

| Отправитель | Получатель | Результат |
|---|---|---|
| alice | bob | доходит без изменений |
| **eve** | **bob** | **отброшено** — bob заблокировал eve |
| alice | carol | доходит, «дурак» → `*****` |
| bob | alice | доходит без изменений |
| carol | alice | доходит без изменений |

Порядок записей между разными ключами может отличаться: топик из трёх партиций,
консьюмер читает их независимо. Внутри одного ключа порядок гарантирован.

> **Про замену.** Слово меняется на столько звёздочек, сколько в нём букв, а
> пунктуация сохраняется — длина текста не меняется. Маскируются только целые
> слова: «дурака» останется как есть.

---

## Шаг 7. Проверить динамику

Здесь проверяется главное требование задания — списки обновляются **без
перезапуска приложения**.

### 7.1. Добавить запрещённое слово

```bash
# bash
echo "шучу:1" | docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=:
```

```powershell
# PowerShell
'шучу:1' | docker exec -i practicum3-kafka kafka-console-producer --topic banned_words --bootstrap-server kafka:29092 --property parse.key=true --property key.separator=:
```

В логе: `Слово добавлено в справочник: шучу`.

Отправьте сообщение с этим словом и сравните результат:

```bash
# bash
echo 'carol:{"user_id":"alice","recipient_id":"carol","message":"ты дурак, шучу :)","timestamp":"2026-09-16T11:00:00Z"}' | \
  docker exec -i practicum3-kafka kafka-console-producer \
    --topic messages --bootstrap-server kafka:29092 \
    --property parse.key=true --property key.separator=:
```

```powershell
# PowerShell
'carol:{"user_id":"alice","recipient_id":"carol","message":"ты дурак, шучу :)","timestamp":"2026-09-16T11:00:00Z"}' | docker exec -i practicum3-kafka kafka-console-producer --topic messages --bootstrap-server kafka:29092 --property parse.key=true --property key.separator=:
```

Теперь замаскированы оба слова: `ты ***** **** :)`.

### 7.2. Убрать слово (tombstone)

Удаление — запись с пустым значением. Ключи `null.marker` и `parse.null`
объясняют продюсеру, что пустая правая часть означает `null`, а не пустую строку.

```bash
# bash
echo "дурак:" | docker exec -i practicum3-kafka kafka-console-producer \
  --topic banned_words --bootstrap-server kafka:29092 \
  --property parse.key=true --property key.separator=: \
  --property null.marker= --property parse.null=true
```

```powershell
# PowerShell
'дурак:' | docker exec -i practicum3-kafka kafka-console-producer --topic banned_words --bootstrap-server kafka:29092 --property parse.key=true --property key.separator=: --property null.marker= --property parse.null=true
```

В логе: `Слово удалено из справочника: дурак`. Повторите отправку сообщения из
7.1 — «дурак» больше не маскируется, остаётся только `шучу` → `****`.

### 7.3. Заблокировать пользователя на лету

```bash
# bash
echo 'carol:{"blocker_id":"carol","blocked_id":"alice","action":"block"}' | \
  docker exec -i practicum3-kafka kafka-console-producer \
    --topic blocked_users --bootstrap-server kafka:29092 \
    --property parse.key=true --property key.separator=:
```

```powershell
# PowerShell
'carol:{"blocker_id":"carol","blocked_id":"alice","action":"block"}' | docker exec -i practicum3-kafka kafka-console-producer --topic blocked_users --bootstrap-server kafka:29092 --property parse.key=true --property key.separator=:
```

В логе: `carol заблокировал alice`. Следующее сообщение от alice к carol уже не
дойдёт — в логе появится `Сообщение отброшено: alice заблокирован у carol`.

Отменяется командой `unblock` с тем же ключом:

```bash
echo 'carol:{"blocker_id":"carol","blocked_id":"alice","action":"unblock"}' | \
  docker exec -i practicum3-kafka kafka-console-producer \
    --topic blocked_users --bootstrap-server kafka:29092 \
    --property parse.key=true --property key.separator=:
```

### 7.4. Проверить, что состояние переживает перезапуск

```bash
docker compose --profile apps restart messaging-streams-app
docker compose logs -f messaging-streams-app
```

После старта отправьте сообщение от `eve` к `bob` — оно всё так же отбрасывается,
**без повторной заливки справочников**: persistent state store лежит на диске
в томе `streams-state` и восстанавливается из changelog-топиков.

### 7.5. Автотесты (без брокера)

Топология проверяется через `TopologyTestDriver` — Kafka для этого не нужна.

```bash
# bash
cd src
docker run --rm -v "$(pwd):/work" -w /work gradle:8.7-jdk21 gradle --no-daemon test
cd ..
```

```powershell
# PowerShell
cd src
docker run --rm -v "${PWD}:/work" -w /work gradle:8.7-jdk21 gradle --no-daemon test
cd ..
```

Ожидается `BUILD SUCCESSFUL`, 11 тестов: маскирование слов (5) и топология
целиком (6) — блокировка, разблокировка, цензура, tombstone, обе стадии вместе,
содержимое state store.

### 7.6. Веб-интерфейс (необязательно)

Посмотреть топики, содержимое записей, группы и лаг глазами:

```bash
docker compose --profile tools up -d kafka-ui
```

Открыть http://localhost:8080 — кластер `practicum-3`.

---

## Шаг 8. Аналитика ksqlDB (задание 2)

Дополнительное задание, поднимается отдельным профилем. Выполняется поверх уже
залитых на шаге 5 сообщений.

```bash
docker compose --profile ksqldb up -d
```

Серверу нужно 30–60 секунд на старт. Дождитесь готовности:

```bash
docker compose --profile ksqldb logs ksqldb-server | grep -i "ksqlDB Server up"
```

```powershell
docker compose --profile ksqldb logs ksqldb-server | Select-String "ksqlDB Server up"
```

Подключиться к CLI:

```bash
docker exec -it practicum3-ksqldb-cli ksql http://ksqldb-server:8088
```

Приглашение `ksql>` означает, что можно выполнять запросы из
[ksqldb-queries.sql](ksqldb-queries.sql) **по порядку** — копируя блок за блоком.
Создаются поток `messages_stream` и три таблицы.

Проверочные запросы и ожидаемые значения на тестовых данных:

```sql
SELECT * FROM total_messages WHERE metric_key = 1;           -- 5
SELECT * FROM total_unique_recipients WHERE metric_key = 1;  -- 3 (bob, carol, alice)
SELECT * FROM user_statistics WHERE user_id = 'alice';       -- 2 сообщения, 2 получателя
```

Полная таблица статистики:

| user_id | messages_sent | unique_recipients |
|---|---|---|
| alice | 2 | 2 (bob, carol) |
| eve | 1 | 1 (bob) |
| bob | 1 | 1 (alice) |
| carol | 1 | 1 (alice) |

Если вы отправляли дополнительные сообщения на шаге 7, числа будут больше —
аналитика считает по топику `messages`, то есть по всем отправленным сообщениям,
включая отброшенные блокировкой.

Выйти из CLI — `exit`. Push-запрос (`EMIT CHANGES` без `WHERE`) прерывается
`Ctrl+C`.

---

## Остановка и очистка

```bash
# Только приложение, брокер остаётся работать
docker compose --profile apps rm -sf messaging-streams-app

# Всё, данные в томах сохраняются
docker compose down

# Всё вместе с данными и состоянием — следующий запуск начнётся с шага 2
docker compose down -v
```

Если поднимались дополнительные профили, укажите их, иначе контейнеры останутся:

```bash
docker compose --profile apps --profile ksqldb --profile tools down -v
```

---

## Если что-то пошло не так

### `Connection refused` при создании топиков

Брокер ещё не готов. Проверьте `docker compose ps` — нужен статус `(healthy)`,
а не `(health: starting)`. Вернитесь к [шагу 1](#шаг-1-поднять-брокер).

### `Topic 'messages' already exists`

Топики уже созданы — шаг 2 можно пропустить. Если нужно начать с чистого листа:
`docker compose down -v` и заново с шага 1.

### Приложение перезапускается по кругу

```bash
docker compose --profile apps logs --tail 50 messaging-streams-app
```

Частые причины:

- **`UnknownTopicOrPartitionException`** — приложение запущено раньше шага 2.
  Создайте топики и перезапустите: `docker compose --profile apps restart messaging-streams-app`.
- **`Replication factor: 3 larger than available brokers: 1`** — том с данными
  остался от другой конфигурации. Помогает `docker compose down -v`.

### В `filtered_messages` пусто

Проверьте по порядку:

1. Приложение в состоянии `RUNNING` — `docker compose --profile apps logs messaging-streams-app | grep RUNNING`.
2. Сообщения дошли до входного топика — прочитайте `messages` тем же
   `kafka-console-consumer` с `--from-beginning`.
3. Консьюмер запущен с `--from-beginning`: без этого флага он читает только
   новые записи и ничего не покажет.

### Кириллица в консоли — знаки вопроса или мусор

Windows-консоль по умолчанию не в UTF-8. В PowerShell перед чтением:

```powershell
[Console]::OutputEncoding = [Text.Encoding]::UTF8
chcp 65001
```

Если мусор попал в сам топик — данные заливались через `<` из `cmd` или без
`-Encoding utf8`. Перезалейте: `docker compose down -v`, затем с шага 1.

### Консьюмер «висит» и не завершается

Это нормально: `kafka-console-consumer` ждёт новых записей. Прервите `Ctrl+C`
или добавьте `--timeout-ms 10000` — тогда он выйдет сам после паузы.
Сообщение `TimeoutException` в этом случае — штатное завершение.

### Изменения в коде не видны

Образ кэшируется. Пересоберите явно:

```bash
docker compose --profile apps up -d --build --force-recreate messaging-streams-app
```

### Порт 9092 занят

Другой Kafka-стенд (например, из первого модуля) ещё работает. Остановите его
или поменяйте проброс в [docker-compose.yml](docker-compose.yml) на `"9192:9092"`
— на команды внутри контейнера это не влияет, они используют `kafka:29092`.
