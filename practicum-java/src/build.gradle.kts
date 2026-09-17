plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:3.7.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("ch.qos.logback:logback-classic:1.5.6")

    // TopologyTestDriver — прогон топологии без брокера: подать запись на вход,
    // прочитать результат на выходе.
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.7.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

// Корень Gradle-проекта — сама папка src/, поэтому исходники лежат в main/ и test/,
// а не в src/main/ и src/test/, как ожидает раскладка по умолчанию.
sourceSets {
    main {
        java.setSrcDirs(listOf("main/java"))
        resources.setSrcDirs(listOf("main/resources"))
    }
    test {
        java.setSrcDirs(listOf("test/java"))
        resources.setSrcDirs(listOf("test/resources"))
    }
}

application {
    mainClass = "ru.practicum.kafka.streams.MessagingStreamsApp"
}

tasks.test {
    useJUnitPlatform()
}
