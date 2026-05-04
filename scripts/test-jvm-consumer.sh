#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cd "$ROOT"
cargo build -p honker-extension
mvn -q -f packages/honker-jvm/pom.xml install
mvn -q -f packages/honker-kotlin/pom.xml install

mkdir -p "$TMP/src/test/java/consumer" "$TMP/src/test/kotlin/consumer"

cat > "$TMP/pom.xml" <<'POM'
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>consumer</groupId>
  <artifactId>honker-consumer-proof</artifactId>
  <version>1.0-SNAPSHOT</version>

  <properties>
    <kotlin.version>2.2.21</kotlin.version>
    <junit.version>5.11.4</junit.version>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>

  <dependencies>
    <dependency>
      <groupId>dev.honker</groupId>
      <artifactId>honker</artifactId>
      <version>0.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>dev.honker</groupId>
      <artifactId>honker-kotlin</artifactId>
      <version>0.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>${junit.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.jetbrains.kotlin</groupId>
      <artifactId>kotlin-test-junit5</artifactId>
      <version>${kotlin.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.jetbrains.kotlin</groupId>
        <artifactId>kotlin-maven-plugin</artifactId>
        <version>${kotlin.version}</version>
        <executions>
          <execution>
            <id>test-compile</id>
            <phase>test-compile</phase>
            <goals>
              <goal>test-compile</goal>
            </goals>
            <configuration>
              <sourceDirs>
                <sourceDir>${project.basedir}/src/test/kotlin</sourceDir>
              </sourceDirs>
            </configuration>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.5.2</version>
        <configuration>
          <useModulePath>false</useModulePath>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
POM

cat > "$TMP/src/test/java/consumer/ConsumerJavaTest.java" <<'JAVA'
package consumer;

import dev.honker.Database;
import dev.honker.Honker;
import dev.honker.Job;
import dev.honker.Queue;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumerJavaTest {
    @Test
    void opensFromPackagedNativeAndQueuesWork() throws Exception {
        Path dbPath = Files.createTempDirectory("honker-consumer-java").resolve("app.db");
        try (Database db = Honker.open(dbPath)) {
            Queue queue = db.queue("consumer");
            queue.enqueue("{\"from\":\"java\"}");
            Job job = queue.claimOne("worker").orElseThrow();
            assertEquals("{\"from\":\"java\"}", job.payloadJson());
        }
    }
}
JAVA

cat > "$TMP/src/test/kotlin/consumer/ConsumerKotlinTest.kt" <<'KOTLIN'
package consumer

import dev.honker.kotlin.enqueueJson
import dev.honker.kotlin.honker
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

class ConsumerKotlinTest {
    @Test
    fun kotlinWrapperWorksFromCleanConsumer() {
        val dbPath = Files.createTempDirectory("honker-consumer-kotlin").resolve("app.db")
        honker(dbPath).use { db ->
            val queue = db.queue("consumer")
            queue.enqueueJson("""{"from":"kotlin"}""")
            assertEquals("""{"from":"kotlin"}""", queue.claimOne("worker").orElseThrow().payloadJson())
        }
    }
}
KOTLIN

cd "$TMP"
env -u HONKER_EXTENSION_PATH mvn -q test
