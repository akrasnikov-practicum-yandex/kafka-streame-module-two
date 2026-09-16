package ru.practicum.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Команда изменения списка блокировок — запись топика blocked_users.
 *
 * <p>В топик кладутся именно команды, а не готовый список: чтобы заблокировать
 * кого-то, клиенту достаточно знать одну пару «кто кого», а не весь текущий список
 * пользователя. Из потока команд топология сворачивает актуальное множество
 * (см. MessagingTopology), так что состояние восстанавливается проигрыванием топика
 * с начала.
 *
 * <p>Ключ записи Kafka — {@code blocker_id}: он же ключ таблицы блокировок,
 * и именно по нему поток сообщений совмещается с ней без repartition.
 */
public record BlockCommand(
        @JsonProperty("blocker_id") String blockerId,
        @JsonProperty("blocked_id") String blockedId,
        @JsonProperty("action") String action) {

    public static final String ACTION_BLOCK = "block";
    public static final String ACTION_UNBLOCK = "unblock";

    public boolean isBlock() {
        return ACTION_BLOCK.equalsIgnoreCase(action);
    }
}
