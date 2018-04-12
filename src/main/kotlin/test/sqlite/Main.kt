package test.sqlite

import java.util.*
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    val db = Db("test.sqlite.db")

    val random = Random()
    while (true) {
        val count = random.nextInt(20000)

        val values = mutableListOf<Test>()
        for (index in 0 until count) {
            values.add(Test(index, random.nextDouble().toString(), random.nextInt()))
        }

        measureTimeMillis {
            db.insert(values)
        }.apply {
            println("insert #$count: {$this}ms")
        }

        measureTimeMillis {
            val selected = db.select()
        }.apply {
            println("select #$count: {$this}ms")
        }

        measureTimeMillis {
            db.delete(values)
        }.apply {
            println("delete #$count: {$this}ms")
        }
    }
}

