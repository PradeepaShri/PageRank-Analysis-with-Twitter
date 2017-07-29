package org.pagerank.mapper

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import java.io.IOException

class PageRankMapper : Mapper<Text, Text, Text, Text>() {

    private val outKey = Text()
    private val outValue = Text()

    @Override
    @Throws(IOException::class, InterruptedException::class)
    protected fun map(key: Text, value: Text, context: Context) {

        context.write(key, value)

        val node = Node.afterMapReduce(value.toString())
        if (node.getAdjacencyList() != null && node.getAdjacencyList().length > 0) {
            val outPageRank = node.getPageRank() / node.getAdjacencyList().length as Double
            for (i in 0..node.getAdjacencyList().length - 1) {

                val adjacentNOde = node.getAdjacencyList()[i]

                outKey.set(adjacentNOde)

                val adjacentNode = Node().setPageRank(outPageRank)

                outValue.set(adjacentNode.printNode())
                context.write(outKey, outValue)
            }
        }
    }
}
