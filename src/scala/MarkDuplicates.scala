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

import scala.util.control.Breaks.break

import htsjdk.samtools._
import htsjdk.samtools.util.{SortingLongCollection, SortingCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.SAMFileHeaderWritable
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord
import picard.sam.markduplicates.util._

object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {

    var pairSort : SortingCollection[ReadEndsForMarkDuplicates] = new SortingCollection[ReadEndsForMarkDuplicates]
    var fragSort : SortingCollection[ReadEndsForMarkDuplicates] = new SortingCollection[ReadEndsForMarkDuplicates]
    var duplicateIndexes = new SortingLongCollection(100000)
    var numDuplicateIndices : Int = 0

    override def doWork() : Int = {
      var finish : Int = 0
      finish
    }

    def transformRead(input : String) = {
      // Collect data from ADAM via Spark
      println("*** Start to process the ADAM file to collect the information of all the reads into variables! ***")

      val conf = new SparkConf().setAppName("Mark Duplicate").setMaster("spark://10.0.1.2:7077")
      val sc = new ADAMContext(new SparkContext(conf))
      var readsrdd: RDD[AlignmentRecord] = sc.loadAlignments(input)

      var header: SAMFileHeader
      var libraryIdGenerator = new LibraryIdGenerator(header)
      var tmp : java.util.ArrayList[AlignmentRecord] = new util.ArrayList[AlignmentRecord]
      var index : Long = 0

      // Iterate the data and transform into new variables
      for (rec : AlignmentRecord <- readsrdd.collect()) {
        if (rec.getReadMapped) {
          if (rec.getContig.getReferenceIndex == -1)
            break()
      } else if (!rec.getSecondaryAlignment && !rec.getSupplementaryAlignment) {
          var fragmentEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, index, rec, libraryIdGenerator)
          fragSort.add(fragmentEnd)

          if (rec.getReadPaired && rec.getMateMapped) {
            var checkpair : AlignmentRecord = findSecondRead(tmp, rec.getContig.getReferenceIndex, rec.getReadName)
            if (checkpair == null) {
              var pairedEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, index, rec, libraryIdGenerator)
              tmp.add(rec)
            } else {
              var sequence : Int = fragmentEnd.read1IndexInFile.asInstanceOf[Int]
              var coordinate : Int = fragmentEnd.read1Coordinate

              if
            }
          }
        }
      }

    }
    def buildReadEnds(header : SAMFileHeader, index : Long, rec : AlignmentRecord, libraryIdGenerator : LibraryIdGenerator) : ReadEndsForMarkDuplicates = {
      // Build the ReadEnd for each read in ADAM
      val ends: ReadEndsForMarkDuplicates = new ReadEndsForMarkDuplicates()
      val recSAM: SAMRecord = new AlignmentRecordConverter().convert(rec, new SAMFileHeaderWritable(header))

      ends.read1ReferenceIndex = recSAM.getReferenceIndex
      if (recSAM.getReadNegativeStrandFlag) {
        ends.read1Coordinate = recSAM.getUnclippedEnd
        ends.orientation = ReadEnds.R
      }
      else {
        ends.read1Coordinate = recSAM.getUnclippedStart
        ends.orientation = ReadEnds.F
      }
      ends.read1IndexInFile = index
      ends.score = DuplicateScoringStrategy.computeDuplicateScore(recSAM, DUPLICATE_SCORING_STRATEGY)

      if (recSAM.getReadPairedFlag && !recSAM.getMateUnmappedFlag)
        ends.read2ReferenceIndex = recSAM.getMateReferenceIndex.asInstanceOf[Int]

      ends.libraryId = libraryIdGenerator.getLibraryId(recSAM)

      if (opticalDuplicateFinder.addLocationInformation(recSAM.getReadName, ends)) {
        // calculate the RG number (nth in list)
        ends.readGroup = 0
        val rg: String = recSAM.getAttribute("RG").toString
        val readGroups: java.util.List[SAMReadGroupRecord] = header.getReadGroups

        if (rg != null && readGroups != null) {
          for (readGroup :SAMReadGroupRecord <- readGroups) {
            if (readGroup.getReadGroupId.equals(rg))
              break()
            else ends.readGroup+=1
          }
        }
      }

      ends
    }

    def findSecondRead (tmp : java.util.ArrayList[AlignmentRecord], referindex : Integer, readname : String) : AlignmentRecord = {
      for (target : AlignmentRecord <- tmp) {
        if (target.getContig.getReferenceIndex == referindex && target.getReadName == readname)
          target
      }
      null
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
