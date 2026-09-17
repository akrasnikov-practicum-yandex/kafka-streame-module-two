# Многоступенчатая сборка: SDK нужен только для компиляции, в финальный образ
# попадает лишь среда выполнения с готовым приложением.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Сначала файл проекта — слой с зависимостями переиспользуется, пока они не менялись.
COPY Practicum.Kafka.Streams/Practicum.Kafka.Streams.csproj Practicum.Kafka.Streams/
RUN dotnet restore Practicum.Kafka.Streams/Practicum.Kafka.Streams.csproj

COPY Practicum.Kafka.Streams/ Practicum.Kafka.Streams/
RUN dotnet publish Practicum.Kafka.Streams/Practicum.Kafka.Streams.csproj -c Release -o /app

# Образ на базе Debian, а НЕ alpine: Streamiz хранит persistent state store
# в RocksDB — нативной библиотеке, которой нужен glibc. На alpine с musl
# приложение не стартует.
FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app .

ENTRYPOINT ["dotnet", "Practicum.Kafka.Streams.dll"]
