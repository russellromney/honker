package dev.honker.ormproof

// Kotlin Exposed, as guides/orm/jvm.mdx shows it: raw SQL inside
// transaction { }. The page's snippet inlines its values; this binds
// them, because that is what an app with real data would do and it is
// the path where driver type handling actually shows up.

import dev.honker.HonkerExtension
import java.sql.DriverManager
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import org.sqlite.SQLiteConfig

object ExposedProof {
    @JvmStatic
    fun main(args: Array<String>) {
        val dbPath = System.getenv("HONKER_TEST_DB")
            ?: error("HONKER_TEST_DB is required")

        // Extension loading has to be enabled before the connection
        // opens, which means Exposed's url+driver overload is not
        // enough — it never sees the SQLiteConfig properties and
        // load_extension comes back "not authorized". Hand Exposed a
        // connection factory instead.
        val props = SQLiteConfig().apply { enableLoadExtension(true) }.toProperties()
        Database.connect({
            DriverManager.getConnection("jdbc:sqlite:$dbPath", props).also { conn ->
                conn.createStatement().use { s ->
                    s.execute(
                        "SELECT load_extension('${HonkerExtension.path()}', " +
                            "'${HonkerExtension.entrypoint()}')"
                    )
                    s.execute("SELECT honker_bootstrap()")
                }
            }
        })

        transaction {
            val conn = (this.connection.connection as java.sql.Connection)

            val id = conn.prepareStatement(
                "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id"
            ).use { stmt ->
                stmt.setString(1, "emails_exposed")
                stmt.setString(2, """{"to":"alice@example.com"}""")
                stmt.setInt(3, 0)
                stmt.setInt(4, 3)
                stmt.executeQuery().use { rs -> rs.next(); rs.getLong("id") }
            }
            check(id > 0) { "expected a job id, got $id" }

            val claimed = conn.prepareStatement(
                "SELECT honker_claim_batch(?, ?, ?, ?)"
            ).use { stmt ->
                stmt.setString(1, "emails_exposed")
                stmt.setString(2, "w1")
                stmt.setInt(3, 8)
                stmt.setInt(4, 300)
                stmt.executeQuery().use { rs -> rs.next(); rs.getString(1) }
            }
            check(claimed.contains("\"id\":$id")) { "claimed the wrong job: $claimed" }

            val acked = conn.prepareStatement("SELECT honker_ack(?, ?)").use { stmt ->
                stmt.setLong(1, id)
                stmt.setString(2, "w1")
                stmt.executeQuery().use { rs -> rs.next(); rs.getInt(1) }
            }
            check(acked == 1) { "ack must match the claim" }
        }

        println("PASS jvm-kotlin-exposed")
    }
}
