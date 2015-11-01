///////////////////////////////////////////////////////////////
//***                                                     ***//
//*** Mark Duplicate by using ADAM store structure        ***//
//***                                                     ***//
//***	Created by Qi @ Oct. 2015                           ***//
//***                                                     ***//
//***	CS Department, UCLA                                 ***//
//***                                                     ***//
///////////////////////////////////////////////////////////////

package scala

import java.util

import htsjdk.samtools.{util, SAMFileHeader}
import htsjdk.samtools.util.{SortingLongCollection, SortingCollection}
import picard.sam.markduplicates.util.{LibraryIdGenerator, ReadEndsForMarkDuplicates}

object MarkDuplicates{

    var pairSort : SortingCollection[ReadEndsForMarkDuplicates]
    //var fragSort = new SortingCollection[ReadEndsForMarkDuplicates]
    var dupliateIndexes = new SortingLongCollection()
    //var libraryIdGenerator = new LibraryIdGenerator(SAMFileHeader)

    def transformRead(input : String) = {
      // Collect data from ADAM via Spark
      println("transform reads")

      // Iterate the data and transform into new variables

    }

    def generateDupIndexes() = {
      // Generate the duplicate indexes for remove duplicate reads
      println("Start to generate duplicate indexes")

      var firstOfNextChunk: ReadEndsForMarkDuplicates = null
      var nextChunk: java.util.List[ReadEndsForMarkDuplicates] = new java.util.ArrayList[ReadEndsForMarkDuplicates](200)

      for (next <- pairSort) {
        if (firstOfNextChunk == null) {
          firstOfNextChunk = next
          nextChunk.add(firstOfNextChunk)
        } else if (areComparableForDuplicates(firstOfNextChunk, next, true)) {
          nextChunk.add(next)
        } else {
          if (nextChunk.size() > 1) markDuplicatePairs(nextChunk)
          nextChunk.clear()
          nextChunk.add(next)
          firstOfNextChunk = next
        }
      }

    }

    def writetoADAM(output : String) = {
      // Write results to ADAM file on HDFS
      println("write to ADAM file")

    }

    def main(args : Array[String]) = {
      var input = args(0)
      var output = args(1)
      println("The input ADAM file:  " + input)
      println("The output ADAM file: " + output)

      val t0 = System.nanoTime : Double

      transformRead(input)
      generateDupIndexes()
      writetoADAM(output)

      val t1 = System.nanoTime() : Double

      println("Mark duplicate has been successfully done!")
      println("The total time for Mark Duplicate is " + (t1-t0) / 1000000000.0 + "secs.")
    }
}
