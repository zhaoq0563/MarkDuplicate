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
import picard.sam.markduplicates.util.{ReadEndsForMarkDuplicatesCodec, AbstractMarkDuplicatesCommandLineProgram, LibraryIdGenerator, ReadEndsForMarkDuplicates}

object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {

    var pairSort : SortingCollection[ReadEndsForMarkDuplicates] = new SortingCollection[ReadEndsForMarkDuplicates]
    var fragSort : SortingCollection[ReadEndsForMarkDuplicates] = ()
    var duplicateIndexes = new SortingLongCollection(100000)
    var numDuplicateIndices : Int = 0
    var libraryIdGenerator = null

    override def doWork() : Int = {
      var finish : Int = 0
      finish
    }

    def transformRead(input : String) = {
      // Collect data from ADAM via Spark
      println("Start to process the ADAM file to collect the information of all the reads into variables!")

      // Iterate the data and transform into new variables

    }

    def generateDupIndexes() = {
      // Generate the duplicate indexes for remove duplicate reads
      println("*** Start to generate duplicate indexes! ***")

      var firstOfNextChunk : ReadEndsForMarkDuplicates = null
      val nextChunk = new java.util.ArrayList[ReadEndsForMarkDuplicates](200)

      for (next : ReadEndsForMarkDuplicates <- pairSort) {
        if (firstOfNextChunk == null) {
          firstOfNextChunk = next
          nextChunk.add(firstOfNextChunk)
        } else if (areComparableForDuplicates(firstOfNextChunk, next, true : Boolean)) {
          nextChunk.add(next)
        } else {
          if (nextChunk.size() > 1) markDuplicatePairs(nextChunk)
          nextChunk.clear()
          nextChunk.add(next)
          firstOfNextChunk = next
        }
      }
      if (nextChunk.size() > 1) markDuplicatePairs(nextChunk)
      pairSort.cleanup()
      pairSort = null

      var containsPairs : Boolean = false
      var containsFrags : Boolean = false

      for (next : ReadEndsForMarkDuplicates <- fragSort) {
        if (firstOfNextChunk != null && areComparableForDuplicates(firstOfNextChunk, next, false : Boolean)) {
          nextChunk.add(next)
          containsPairs = containsPairs || next.isPaired()
          containsFrags = containsFrags || !next.isPaired()
        } else {
          if (nextChunk.size() > 1 && containsFrags) markDuplicateFragments(nextChunk, containsPairs)
          nextChunk.clear()
          nextChunk.add(next)
          firstOfNextChunk = next
          containsPairs = next.isPaired()
          containsFrags = !next.isPaired()
        }
      }
      markDuplicateFragments(nextChunk, containsPairs)
      fragSort.cleanup()
      fragSort = null

      println("*** Finish generating duplicate indexes! ***")
      duplicateIndexes.doneAddingStartIteration()
    }

    def areComparableForDuplicates(lhs : ReadEndsForMarkDuplicates, rhs : ReadEndsForMarkDuplicates, compareRead2 : Boolean) : Boolean = {
      var retval: Boolean = (lhs.libraryId == rhs.libraryId) && (lhs.read1ReferenceIndex == rhs.read1ReferenceIndex) && (lhs.read1Coordinate == rhs.read1Coordinate) && (lhs.orientation == rhs.orientation)

      if (retval && compareRead2) {
        retval = (lhs.read2ReferenceIndex == rhs.read2ReferenceIndex) && (lhs.read2Coordinate == rhs.read2Coordinate)
      }

      retval
    }

    def addIndexAsDuplicate(bamIndex : Long) = {
      duplicateIndexes.add(bamIndex)
      numDuplicateIndices += 1
    }

    def markDuplicatePairs(list : java.util.ArrayList[ReadEndsForMarkDuplicates]) = {
      var maxScore : Short = 0
      var best : ReadEndsForMarkDuplicates = null

      /** All read ends should have orientation FF, FR, RF, or RR **/
      for (end : ReadEndsForMarkDuplicates <- list) {
        if (end.score > maxScore || best == null) {
          maxScore = end.score
          best = end
        }
      }

      for (end : ReadEndsForMarkDuplicates <- list) {
        if (end != best) {
          addIndexAsDuplicate(end.read1IndexInFile)
          addIndexAsDuplicate(end.read2IndexInFile)
        }
      }

      // Null for libraryIdGenerator, need to figure out how to get SAM/BAM header
      if (READ_NAME_REGEX != null) {
        AbstractMarkDuplicatesCommandLineProgram.trackOpticalDuplicates(list, opticalDuplicateFinder, libraryIdGenerator)
      }
    }

    def markDuplicateFragments(list : java.util.ArrayList[ReadEndsForMarkDuplicates], containsPairs : Boolean) = {
      if (containsPairs) {
        for (end : ReadEndsForMarkDuplicates <- list) {
          if (!end.isPaired()) addIndexAsDuplicate(end.read1IndexInFile)
        }
      } else {
        var maxScore : Short = 0
        var best : ReadEndsForMarkDuplicates = null
        for (end : ReadEndsForMarkDuplicates <- list) {
          if (end.score > maxScore || best == null) {
            maxScore = end.score
            best = end
          }
        }

        for (end : ReadEndsForMarkDuplicates <- list) {
          if (end != best) {
            addIndexAsDuplicate(end.read1IndexInFile)
          }
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
