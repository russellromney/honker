package dev.honker.ormproof

import dev.honker.HonkerExtension
import java.sql.DriverManager
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.sqlite.SQLiteConfig

object ExposedProof {
    @JvmStatic
    fun main(args: Array<String>) {
        val dbPath = System.getenv("HONKER_TEST_DB")
            ?: error("HONKER_TEST_DB is required")

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
            val conn = TransactionManager.current().connection.connection as java.sql.Connection
            Surface.run(conn, "exp")
        }

        transaction {
            exec("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
            exec("INSERT INTO orders (id, user_id) VALUES (72, 1)")
            exec(
                """
                SELECT honker_enqueue(
                  'exp_atomic',
                  '{"order_id":72}',
                  NULL, NULL, 0, 3, NULL
                )
                """.trimIndent()
            )
        }

        transaction {
            val conn = TransactionManager.current().connection.connection as java.sql.Connection
            conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = 72").use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    check(rs.getInt(1) == 1) { "missing committed order" }
                }
            }
        }

        try {
            transaction {
                exec("INSERT INTO orders (id, user_id) VALUES (73, 1)")
                exec(
                    """
                    SELECT honker_enqueue(
                      'exp_atomic',
                      '{"order_id":73}',
                      NULL, NULL, 0, 3, NULL
                    )
                    """.trimIndent()
                )
                error("rollback")
            }
        } catch (e: Throwable) {
            var t: Throwable? = e
            var isRollback = false
            while (t != null) {
                if (t.message == "rollback") {
                    isRollback = true
                    break
                }
                t = t.cause
            }
            if (!isRollback) throw e
        }

        transaction {
            val conn = TransactionManager.current().connection.connection as java.sql.Connection
            conn.prepareStatement("SELECT COUNT(*) FROM orders WHERE id = 73").use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    check(rs.getInt(1) == 0) { "rollback left an order" }
                }
            }
        }

        println("PASS jvm-kotlin-exposed")
    }
}
