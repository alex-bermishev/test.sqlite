package test.sqlite

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.Types
import java.time.Duration

data class Test(val id: Int,
                val s_value: String? = null,
                val i_value: Int? = null)

class Db(val uri: String) {
    companion object {
        val DEFAULT_STATEMENT_TIMEOUT = Duration.ofSeconds(30)
    }

    private val db by lazy {
        val connection = DriverManager.getConnection("jdbc:sqlite:$uri")
        connection.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE
        connection.autoCommit = false
        val statement = connection.createStatement()

        connection
    }

    init {
        migrate()
    }

    private fun transaction(block: () -> Unit) {
        try {
            block()

            db.commit()
        } catch (ex: Exception) {
            db.rollback()
        }
    }

    private fun createStatement(): Statement {
        val result = db.createStatement()
        result.queryTimeout = DEFAULT_STATEMENT_TIMEOUT.seconds.toInt()

        return result
    }


    private val insertStatement by lazy {
        val sql = """
            INSERT INTO [test] VALUES
            (
                ?,
                ?,
                ?
            )
            """

        db.prepareStatement(sql)
    }

    private val deleteStatement by lazy {
        val sql = """
            DELETE FROM [test]
            WHERE
                [id] = ?
            """

        db.prepareStatement(sql)
    }

    private fun migrate() {
        val sql = """
            CREATE TABLE IF NOT EXISTS [test] (
                [id] INT PRIMARY KEY,
                [s_value] VARCHAR( 100 ),
                [i_value] INT
            ) WITHOUT ROWID;
            """

        transaction {
            val statement = createStatement()
            statement.execute(sql)
        }
    }

    fun insert(values: Collection<Test>) {
        transaction {
            val statement = insertStatement
            for (value in values) {
                insertStatement.setInt(1, value.id)
                if (value.s_value != null)
                    insertStatement.setString(2, value.s_value)
                else
                    insertStatement.setNull(2, Types.VARCHAR)

                if (value.i_value != null)
                    insertStatement.setInt(3, value.i_value)
                else
                    insertStatement.setNull(3, Types.INTEGER)

                statement.executeUpdate()
            }
        }
    }

    fun delete(values: Collection<Test>) {
        transaction {
            val statement = deleteStatement
            for (value in values) {
                statement.setInt(1, value.id)

                statement.executeUpdate()
            }
        }
    }

    fun select(): List<Test> {
        val sql = """
            SELECT
                *
            FROM [test]
        """

        val result = mutableListOf<Test>()

        val statement = createStatement()
        val resultSet = statement.executeQuery(sql)
        while (resultSet.next()) {
            result.add(Test(resultSet.getInt(1),
                    resultSet.getString(2),
                    resultSet.getInt(3)))
        }

        return result
    }
}
