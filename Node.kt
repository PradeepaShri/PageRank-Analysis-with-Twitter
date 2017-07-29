package org.pagerank.node

import org.apache.commons.lang.StringUtils

import java.io.IOException
import java.util.Arrays

class Node {

    private var pageRank = 0.25
    private var adjacencyList: Array<String>? = null

    fun getPageRank(): Double {
        return pageRank
    }

    fun setPageRank(pageRank: Double): Node {
        this.pageRank = pageRank
        return this
    }

    fun getAdjacencyList(): Array<String>? {
        return adjacencyList
    }

    fun setAdjacencyList(adjacencyList: Array<String>): Node {
        this.adjacencyList = adjacencyList
        return this
    }

    val isNode: Boolean
        get() = adjacencyList != null

    fun printNode(): String {
        val sb = StringBuilder()
        sb.append(pageRank)

        if (getAdjacencyList() != null) {
            sb.append('\t').append(StringUtils.join(getAdjacencyList(), '\t'))
        }
        return sb.toString()
    }

    companion object {

        @Throws(IOException::class)
        fun afterMapReduce(value: String): Node {
            val parts = StringUtils.splitPreserveAllTokens(value, '\t')
            if (parts.size < 1) {
                throw IOException()
            }
            val node = Node().setPageRank(Double.valueOf(parts[0]))
            if (parts.size > 1) {
                node.setAdjacencyList(Arrays.copyOfRange(parts, 1, parts.size))
            }
            return node
        }
    }
}
