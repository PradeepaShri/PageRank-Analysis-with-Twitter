import org.apache.commons.io.*
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.*
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.*
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.io.*
import java.util.*

object PageRank {

    internal var map = TreeMap<String, String>()

    @Throws(Exception::class)
    fun main(args: Array<String>) {

        val inputPath = args[0]
        val outputPath = args[1]

        iterativeMapReduce(inputPath, outputPath)
    }

    @Throws(Exception::class)
    fun iterativeMapReduce(inputPath: String, outputDir: String) {

        var iteration = 1
        val desiredConvergence = 0.005
        var jobOutputPath: Path? = null
        val conf = Configuration()
        val out = Path(outputDir)
        if (out.getFileSystem(conf).exists(out))
            out.getFileSystem(conf).delete(out, true)
        out.getFileSystem(conf).mkdirs(out)

        var `in` = Path(out, "initialInput.txt")
        val infile = Path(inputPath)

        val fs = infile.getFileSystem(conf)
        val noOfNodes = getNoOfNodes(infile)
        val initialPageRank = 1.0 / noOfNodes.toDouble()

        val os = fs.create(`in`)
        val iter = IOUtils.lineIterator(fs.open(infile), "UTF8")
        while (iter.hasNext()) {
            val line = iter.nextLine()
            val parts = line.toString().split(" ")

            val node = Node().setPageRank(initialPageRank)
                    .setAdjacencyList(
                            Arrays.copyOfRange(parts, 1, parts.size))
            IOUtils.write(parts[0] + '\t' + node.printNode() + '\n', os)
        }
        os.close()
        while (true) {

            jobOutputPath = Path(out, String.valueOf(iteration))
            conf.setInt(PageRankReducer.NO_OF_NODES, noOfNodes)
            val job = Job.getInstance(conf)
            job.setJarByClass(PageRank::class.java)
            job.setMapperClass(PageRankMapper::class.java)
            job.setReducerClass(PageRankReducer::class.java)

            job.setInputFormatClass(KeyValueTextInputFormat::class.java)

            job.setMapOutputKeyClass(Text::class.java)
            job.setMapOutputValueClass(Text::class.java)

            FileInputFormat.setInputPaths(job, `in`)
            FileOutputFormat.setOutputPath(job, jobOutputPath)

            if (!job.waitForCompletion(true)) {
                throw Exception("Job failed")
            }
            val sumOfConvergence = job.getCounters()
                    .findCounter(PageRankReducer.Counter.CDELTA).getValue()
            val convergence = sumOfConvergence.toDouble() / PageRankReducer.CSF / noOfNodes.toDouble()

            if (convergence < desiredConvergence) {
                break
            }
            `in` = jobOutputPath
            iteration++
        }

        sortAndListTopTenPages(jobOutputPath, out)

    }

    @Throws(IOException::class)
    fun sortAndListTopTenPages(outputPath: Path, out: Path) {
        val conf = Configuration()
        val fs = outputPath.getFileSystem(conf)
        val cs = fs.getContentSummary(outputPath)
        val fileCount = cs.getFileCount()
        for (i in 0..fileCount - 1) {
            val file = Path(outputPath, "part-r-0000" + Integer.toString(i))
            val iter = IOUtils.lineIterator(fs.open(file), "UTF8")
            while (iter.hasNext()) {
                val line = iter.nextLine()
                val parts = line.toString().split(" ")
                if (parts.size == 1) {
                    map.put(Integer.toString(0), parts[0])
                } else {
                    map.put(parts[1], parts[0])
                }
            }
        }

        val output = Path(out, "sortedPageRanks.txt")
        val os = fs.create(output)
        for (i in 10 downTo 1) {
            val entry = map.pollLastEntry()
            IOUtils.write(entry.getValue() + ',' + entry.getKey() + '\n', os)
        }
        os.close()

    }

    @Throws(IOException::class)
    fun getNoOfNodes(file: Path): Int {
        val conf = Configuration()
        val fs = file.getFileSystem(conf)

        return IOUtils.readLines(fs.open(file), "UTF8").size()

    }
}
