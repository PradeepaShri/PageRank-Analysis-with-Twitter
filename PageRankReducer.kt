package org.pagerank.reducer

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import java.io.IOException

class PageRankReducer : Reducer<Text, Text, Text, Text>() {
    private var totalNoOfNodes: Int = 0

    enum class Counter {
        CDELTA
    }

    @Override
    @Throws(IOException::class, InterruptedException::class)
    protected fun setup(context: Context) {
        totalNoOfNodes = context.getConfiguration().getInt(
                NO_OF_NODES, 0)
    }

    private val outValue = Text()

    @Throws(IOException::class, InterruptedException::class)
    fun reduce(key: Text, values: Iterable<Text>, context: Context) {

        var sum = 0.0
        val dampingFactor = (1.0 - DF) / totalNoOfNodes.toDouble()
        var realNode = Node()
        for (textValue in values) {

            val node = Node.afterMapReduce(textValue.toString())
            if (node.isNode()) {
                realNode = node
            } else {
                sum += node.getPageRank()
            }
        }

        val newPageRank = dampingFactor + DF * sum

        val delta = realNode.getPageRank() - newPageRank

        realNode.setPageRank(newPageRank)

        outValue.set(realNode.printNode())
        context.write(key, outValue)

        val scaledDelta = Math.abs((delta * CSF).toInt())

        context.getCounter(Counter.CDELTA).increment(scaledDelta)
    }

    companion object {

        val CSF = 1000.0
        val DF = 0.85
        var NO_OF_NODES = "pagerank.numnodes"
    }
}
