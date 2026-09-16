# Модуль 2. Kafka Streams — Яндекс Практикум

Учебные работы второго модуля курса «Apache Kafka».

## Практическая работа 3 — сервис обмена сообщениями с блокировкой и цензурой

Потоковая обработка сообщений: пользователи блокируют друг друга, запрещённые слова
маскируются, списки блокировок и слов живут в persistent state store и обновляются
на лету. Плюс аналитика потока через ksqlDB.

Работа выполнена **в двух вариантах**, каждый в своей ветке — одна и та же топология
на эталонной библиотеке и на её .NET-порту:

| Ветка | Стек | Что смотреть |
|---|---|---|
| [`practicum-java`](../../tree/practicum-java/practicum-java) | Java 21 + Apache Kafka Streams | [practicum-java/README.md](../../blob/practicum-java/practicum-java/README.md) |
| [`practicum-dotnet`](../../tree/practicum-dotnet/practicum-dotnet) | .NET 8 + Streamiz.Kafka.Net | [practicum-dotnet/README.md](../../blob/practicum-dotnet/practicum-dotnet/README.md) |

Обе ветки решают оба задания работы:

- **Задание 1** (обязательное) — блокировка нежелательных пользователей и цензура слов
  на потоке сообщений, топология с persistent state store.
- **Задание 2** (дополнительное) — анализ и агрегирование сообщений через ksqlDB.

Ветки независимы и в `main` не сливаются: `main` хранит только этот индекс, каждая
реализация целиком лежит в своей ветке.

Термины Kafka Streams и ksqlDB с расшифровкой — в [glossary.md](glossary.md).

Предыдущий модуль (кластер, продюсер и консьюмеры) —
[kafka-streame-module-one](https://github.com/akrasnikov-practicum-yandex/kafka-streame-module-one).
