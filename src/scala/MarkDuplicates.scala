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
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary, SAMFileHeaderWritable}
import org.bdgenomics.adam.rdd.{ADAMSpecificRecordSequenceDictionaryRDDAggregator, ADAMContext}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDDFunctions
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

    def buildFragSort(rec : AlignmentRecord, index : Long, header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator) : ReadEndsForMarkDuplicates = {
      if (rec.getReadMapped == false) {
        if (rec.getContig.getReferenceIndex == -1)
          null
      } else if (!rec.getSecondaryAlignment && !rec.getSupplementaryAlignment) {
        val fragmentEnd: ReadEndsForMarkDuplicates = buildReadEnds(header, index, rec, libraryIdGenerator)
        fragmentEnd
      }
      null
    }

    def buildPairSort(rec : AlignmentRecord, index : Long, header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator) : ReadEndsForMarkDuplicates = {
      val tmp: java.util.ArrayList[AlignmentRecord] = new util.ArrayList[AlignmentRecord]

      if (rec.getReadMapped) {
        if (rec.getContig.getReferenceIndex == -1)
          null
      } else if (!rec.getSecondaryAlignment && !rec.getSupplementaryAlignment) {
        if (rec.getReadPaired && rec.getMateMapped) {
          var checkpair : AlignmentRecord = findSecondRead(tmp, rec.getContig.getReferenceIndex, rec.getReadName)
          if (checkpair == null) {
            tmp.add(rec)
            null
          } else {
            val pairedEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, index, checkpair, libraryIdGenerator)
            val currentEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, index, rec, libraryIdGenerator)
            val sequence : Int = currentEnd.read1IndexInFile.asInstanceOf[Int]
            val coordinate : Int = currentEnd.read1Coordinate

            // Why this function has been removed?????? = does not matter
            //if (rec.getFirstOfPair) {
            pairedEnd.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(Boolean2boolean(rec.getReadNegativeStrand), pairedEnd.orientation == ReadEnds.R)
            //} else {
            //  pairedEnd.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(pairedEnd.orientation == ReadEnds.R, Boolean2boolean(rec.getReadNegativeStrand))
            //}

            if (sequence > pairedEnd.read1ReferenceIndex || (sequence == pairedEnd.read1ReferenceIndex && coordinate >= pairedEnd.read1Coordinate)) {
              pairedEnd.read2ReferenceIndex = sequence
              pairedEnd.read2Coordinate = coordinate
              pairedEnd.read2IndexInFile = index
              pairedEnd.orientation = ReadEnds.getOrientationByte(pairedEnd.orientation == ReadEnds.R, rec.getReadNegativeStrand)
            } else {
              pairedEnd.read2ReferenceIndex = pairedEnd.read1ReferenceIndex
              pairedEnd.read2Coordinate = pairedEnd.read1Coordinate
              pairedEnd.read2IndexInFile = pairedEnd.read1IndexInFile
              pairedEnd.read1ReferenceIndex = sequence
              pairedEnd.read1Coordinate = coordinate
              pairedEnd.read1IndexInFile = index
              pairedEnd.orientation = ReadEnds.getOrientationByte(rec.getReadNegativeStrand, pairedEnd.orientation == ReadEnds.R)
            }

            pairedEnd.score += DuplicateScoringStrategy.computeDuplicateScore(new AlignmentRecordConverter().convert(rec, new SAMFileHeaderWritable(header)), DUPLICATE_SCORING_STRATEGY)
            pairedEnd
          }
        }
        null
      }
      null
    }

    def transformRead(input : String, readsrdd : RDD[AlignmentRecord], header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator) = {
      // Collect data from ADAM via Spark
      println("*** Start to process the ADAM file to collect the information of all the reads into variables! ***")

      val tmp: java.util.ArrayList[AlignmentRecord] = new util.ArrayList[AlignmentRecord]
      var index : Long = 0

      val readADAMRDD = readsrdd.zipWithIndex().map{case (read : AlignmentRecord, index : Long) => {
        val fragmentEnd = buildFragSort(read, index, header, libraryIdGenerator)
        val pairedEnd = buildPairSort(read, index, header, libraryIdGenerator)
        (fragmentEnd, pairedEnd)
      }}

      for (readFrag <- readADAMRDD.map{case (fragmentEnd : ReadEndsForMarkDuplicates, pairedEnd : ReadEndsForMarkDuplicates) => fragmentEnd }.filter(fragmentEnd => !fragmentEnd.eq(null)).collect()){
        fragSort.add(readFrag)
      }
      fragSort.doneAdding()

      for (readPair <- readADAMRDD.map{case (fragmentEnd : ReadEndsForMarkDuplicates, pairedEnd : ReadEndsForMarkDuplicates) => pairedEnd}.filter(pairedEnd => !pairedEnd.eq(null)).collect()){
        pairSort.add(readPair)
      }
      pairSort.doneAdding()


      /*
      // Iterate the data and transform into new variables
      for (rec : AlignmentRecord <- readsrdd.collect()) {
        if (rec.getReadMapped) {
          if (rec.getContig.getReferenceIndex == -1)
            break()
        } else if (!rec.getSecondaryAlignment && !rec.getSupplementaryAlignment) {
          var fragmentEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, index, rec, libraryIdGenerator)
          fragSort.add(fragmentEnd)

          if (rec.getReadPaired && rec.getMateMapped) {
            val checkpair : AlignmentRecord = findSecondRead(tmp, rec.getContig.getReferenceIndex, rec.getReadName)
            if (checkpair == null) {
              var pairedEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, index, rec, libraryIdGenerator)
              tmp.add(rec)
            } else {
              var sequence : Int = fragmentEnd.read1IndexInFile.asInstanceOf[Int]
              var coordinate : Int = fragmentEnd.read1Coordinate
              var pairedEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, index, rec, libraryIdGenerator)

              // Why this function has been removed?????? = does not matter
              //if (rec.getFirstOfPair) {
              pairedEnd.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(Boolean2boolean(rec.getReadNegativeStrand), pairedEnd.orientation == ReadEnds.R)
              //} else {
              //  pairedEnd.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(pairedEnd.orientation == ReadEnds.R, Boolean2boolean(rec.getReadNegativeStrand))
              //}

              if (sequence > pairedEnd.read1ReferenceIndex || (sequence == pairedEnd.read1ReferenceIndex && coordinate >= pairedEnd.read1Coordinate)) {
                pairedEnd.read2ReferenceIndex = sequence
                pairedEnd.read2Coordinate = coordinate
                pairedEnd.read2IndexInFile = index
                pairedEnd.orientation = ReadEnds.getOrientationByte(pairedEnd.orientation == ReadEnds.R, rec.getReadNegativeStrand)
              } else {
                pairedEnd.read2ReferenceIndex = pairedEnd.read1ReferenceIndex
                pairedEnd.read2Coordinate = pairedEnd.read1Coordinate
                pairedEnd.read2IndexInFile = pairedEnd.read1IndexInFile
                pairedEnd.read1ReferenceIndex = sequence
                pairedEnd.read1Coordinate = coordinate
                pairedEnd.read1IndexInFile = index
                pairedEnd.orientation = ReadEnds.getOrientationByte(rec.getReadNegativeStrand, pairedEnd.orientation == ReadEnds.R)
              }

              pairedEnd.score += DuplicateScoringStrategy.computeDuplicateScore(new AlignmentRecordConverter().convert(rec, new SAMFileHeaderWritable(header)), DUPLICATE_SCORING_STRATEGY)
              pairSort.add(pairedEnd)
            }
          }
        }
        index += 1
      }*/

      println("*** Finish building pairSort and fragSort! ***")
    }

    def buildReadEnds(header : SAMFileHeader, index : Long, rec : AlignmentRecord, libraryIdGenerator : LibraryIdGenerator) : ReadEndsForMarkDuplicates = {
      // Build the ReadEnd for each read in ADAM
      val ends: ReadEndsForMarkDuplicates = new ReadEndsForMarkDuplicates()
      val recSAM: SAMRecord = new AlignmentRecordConverter().convert(rec, new SAMFileHeaderWritable(header))

      ends.read1ReferenceIndex = Integer2int(recSAM.getReferenceIndex)
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
      // Find out the paired read that was already been processed but did not find their mate
      // Return the read or return null if no finding
      for (target : AlignmentRecord <- tmp) {
        if (target.getContig.getReferenceIndex == referindex && target.getReadName == readname)
          target
      }
      null
    }

    def generateDupIndexes(libraryIdGenerator : LibraryIdGenerator) = {
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
          if (nextChunk.size() > 1) markDuplicatePairs(nextChunk, libraryIdGenerator)
          nextChunk.clear()
          nextChunk.add(next)
          firstOfNextChunk = next
        }
      }
      if (nextChunk.size() > 1) markDuplicatePairs(nextChunk, libraryIdGenerator)
      pairSort.cleanup()
      pairSort = null

      var containsPairs : Boolean = false
      var containsFrags : Boolean = false

      for (next : ReadEndsForMarkDuplicates <- fragSort) {
        if (firstOfNextChunk != null && areComparableForDuplicates(firstOfNextChunk, next, false : Boolean)) {
          nextChunk.add(next)
          containsPairs = containsPairs || next.isPaired
          containsFrags = containsFrags || !next.isPaired
        } else {
          if (nextChunk.size() > 1 && containsFrags) markDuplicateFragments(nextChunk, containsPairs)
          nextChunk.clear()
          nextChunk.add(next)
          firstOfNextChunk = next
          containsPairs = next.isPaired
          containsFrags = !next.isPaired
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

    def markDuplicatePairs(list : java.util.ArrayList[ReadEndsForMarkDuplicates], libraryIdGenerator : LibraryIdGenerator) = {
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
          if (!end.isPaired) addIndexAsDuplicate(end.read1IndexInFile)
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

    def writetoADAM(output : String, readsrdd : RDD[AlignmentRecord], sc : ADAMContext) = {
      // Write results to ADAM file on HDFS
      println("*** Start to write reads back to ADAM file without duplicates! ***")

      // Transform duplicateIndexes into HashTable
      val dpIndexes = new util.Hashtable[Long, Long]()
      for (index : Long <- duplicateIndexes) {
        dpIndexes.put(index, index)
      }

      val broadcastDpIndexes = sc.


      var indexInFile : Long = 0
      var nextDuplicateIndex : Long = 0
      if (duplicateIndexes.hasNext) {
        nextDuplicateIndex = duplicateIndexes.next()
      } else {
        nextDuplicateIndex = -1
      }

      // broadcast(list)    hash_tbl    record.field in hash_tbl
      for (read <- readsrdd.collect()) {
        if (indexInFile == nextDuplicateIndex) {
          read.setDuplicateRead(true)
          if (duplicateIndexes.hasNext) {
            nextDuplicateIndex = duplicateIndexes.next()
          } else {
            nextDuplicateIndex = -1
          }
        } else {
          read.setDuplicateRead(false)
        }
        indexInFile += 1
      }

      // Use the filter function to get rid of those reads contains indexes in the duplicateIndexes
      readsrdd.filter(read => read.getDuplicateRead.eq(false)).adamParquetSave(output)
    }

    def main(args : Array[String]) = {
      val input = args(0)
      val output = args(1)
      val headerinput = args(2)
      println("*** The directory of input ADAM file             : " + input)
      println("*** The directory of output ADAM file            : " + output)
      println("*** The directory of input Fasta file for header : " + headerinput)

      val t0 = System.nanoTime : Double

      val conf = new SparkConf().setAppName("Mark Duplicate").setMaster("spark://10.0.1.2:7077")
      val sc = new ADAMContext(new SparkContext(conf))
      val readsrdd: RDD[AlignmentRecord] = sc.loadAlignments(input)
      // Get the header for building (pair/frag)Sort
      val sd: SequenceDictionary = new ADAMSpecificRecordSequenceDictionaryRDDAggregator(readsrdd).adamGetSequenceDictionary(false)
      val rgd: RecordGroupDictionary = new AlignmentRecordRDDFunctions(readsrdd).adamGetReadGroupDictionary()
      val header: SAMFileHeader = new AlignmentRecordConverter().createSAMHeader(sd, rgd)
      val libraryIdGenerator = new LibraryIdGenerator(header)

      transformRead(input, readsrdd, header, libraryIdGenerator)
      generateDupIndexes(libraryIdGenerator)
      writetoADAM(output, readsrdd, sc)

      val t1 = System.nanoTime() : Double

      println("*** Mark duplicate has been successfully done! ***")
      println("*** The total time for Mark Duplicate is : " + (t1-t0) / 1000000000.0 + "secs.")
    }
}
