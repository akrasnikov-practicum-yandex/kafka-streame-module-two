# Многоступенчатая сборка: JDK нужен только для компиляции, в финальный образ
# попадает лишь JRE с готовым приложением.
FROM gradle:8.7-jdk21 AS build
WORKDIR /build

# Сначала файлы сборки — слой с зависимостями переиспользуется, пока они не менялись.
COPY settings.gradle.kts build.gradle.kts ./
RUN gradle --no-daemon dependencies --quiet || true

COPY main ./main
# Тесты в образе не гоняем: сборка должна быть воспроизводимой и быстрой,
# а тесты запускаются отдельно командой gradle test.
RUN gradle --no-daemon installDist -x test

FROM eclipse-temurin:21-jre
WORKDIR /app

# RocksDB (движок persistent state store) — нативная библиотека, которой нужен
# glibc. Образ на базе Debian её содержит; на alpine с musl приложение не стартует.
COPY --from=build /build/build/install/practicum-messaging-streams/ ./

ENTRYPOINT ["./bin/practicum-messaging-streams"]
