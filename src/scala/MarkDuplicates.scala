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

import htsjdk.samtools.DuplicateScoringStrategy.ScoringStrategy

import scala.util.{ReadEndsMDComparator, CSAlignmentRecord}
import scala.util.control.Breaks.break

import htsjdk.samtools._
import htsjdk.samtools.util.{SortingLongCollection, SortingCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary, SAMFileHeaderWritable}
import org.bdgenomics.adam.rdd.{ADAMRDDFunctions, ADAMSpecificRecordSequenceDictionaryRDDAggregator, ADAMContext}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDDFunctions
import org.bdgenomics.formats.avro.AlignmentRecord
import picard.sam.markduplicates.util._

object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {

    var maxInMemory : Int = (Runtime.getRuntime.maxMemory() * 0.5 / ReadEndsForMarkDuplicates.SIZE_OF).toInt
    var pairSort : SortingCollection[ReadEndsForMarkDuplicates] = new SortingCollection[ReadEndsForMarkDuplicates](ReadEndsForMarkDuplicates, new ReadEndsForMarkDuplicatesCodec(), new ReadEndsMDComparator(), maxInMemory, this.TMP_DIR)
    var fragSort : SortingCollection[ReadEndsForMarkDuplicates] = new SortingCollection[ReadEndsForMarkDuplicates](ReadEndsForMarkDuplicates, new ReadEndsForMarkDuplicatesCodec(), new ReadEndsMDComparator(), maxInMemory, this.TMP_DIR)
    var duplicateIndexes = new SortingLongCollection(100000)
    var numDuplicateIndices : Int = 0

    override def doWork() : Int = {
      var finish : Int = 0
      finish
    }

    // Transform the rdd in ADAM format to our self customized format to build pair/frag Sort list
    def buildSortList(input : String, readsrdd : RDD[AlignmentRecord], header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator) = {
      println("*** Start to process the ADAM file to collect the information of all the reads into variables! ***")

      val tmp: java.util.ArrayList[CSAlignmentRecord] = new java.util.ArrayList[CSAlignmentRecord]

      // Map the ADAMrdd[AlignmentRecord] to CSrdd[CSRecord] with index
      val readCSIndexRDD = readsrdd.zipWithIndex().map{case (read : AlignmentRecord, index : Long) => {
        val CSRecord : CSAlignmentRecord = buildCSAlignmentRecord(read, index, header, libraryIdGenerator)

        CSRecord
      }}

      // Collect the data from CSrdd and iterate them to build frag/pair Sort
      for (readCSRecord <- readCSIndexRDD.collect()) {
        if (readCSRecord.getReadUnmappedFlag) {
          if (readCSRecord.getReferenceIndex == -1) {
            break()
          }
        } else if (!readCSRecord.isSecondaryOrSupplementary) {
          val fragmentEnd = buildReadEnds(header, readCSRecord, libraryIdGenerator)
          fragSort.add(fragmentEnd)

          if (readCSRecord.getReadPairedFlag && !readCSRecord.getMateUnmappedFlag) {
            val checkPair = findMate(tmp, readCSRecord.getReferenceIndex, readCSRecord.getReadName)
            if (checkPair == null) {
              tmp.add(readCSRecord)
            } else {
              val sequence : Int = fragmentEnd.read1IndexInFile.asInstanceOf[Int]
              val coordinate : Int = fragmentEnd.read1Coordinate
              val pairedEnd : ReadEndsForMarkDuplicates = buildReadEnds(header, checkPair, libraryIdGenerator)

              if (readCSRecord.getFirstOfPairFlag) {
                pairedEnd.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, pairedEnd.orientation == ReadEnds.R)
              } else {
                pairedEnd.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(pairedEnd.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
              }

              if (sequence > pairedEnd.read1ReferenceIndex || (sequence == pairedEnd.read1ReferenceIndex && coordinate >= pairedEnd.read1Coordinate)) {
                pairedEnd.read2ReferenceIndex = sequence
                pairedEnd.read2Coordinate = coordinate
                pairedEnd.read2IndexInFile = readCSRecord.getIndex
                pairedEnd.orientation = ReadEnds.getOrientationByte(pairedEnd.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
              } else {
                pairedEnd.read2ReferenceIndex = pairedEnd.read1ReferenceIndex
                pairedEnd.read2Coordinate = pairedEnd.read1Coordinate
                pairedEnd.read2IndexInFile = pairedEnd.read1IndexInFile
                pairedEnd.read1ReferenceIndex = sequence
                pairedEnd.read1Coordinate = coordinate
                pairedEnd.read1IndexInFile = readCSRecord.getIndex
                pairedEnd.orientation = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, pairedEnd.orientation == ReadEnds.R)
              }

              pairedEnd.score += readCSRecord.getScore
              pairSort.add(pairedEnd)
            }
          }
        }
      }

      /*
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
      pairSort.doneAdding()*/

      fragSort.doneAdding()
      pairSort.doneAdding()

      println("*** Finish building pairSort and fragSort! ***")
    }

    def buildCSAlignmentRecord(read : AlignmentRecord, index : Long, header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator) : CSAlignmentRecord = {
      // Build one single CSAlignmentRecord for CSrdd with index
      val CSRecord : CSAlignmentRecord = new CSAlignmentRecord()
      val readSAM : SAMRecord = new AlignmentRecordConverter().convert(read, new SAMFileHeaderWritable(header))

      // Assign the flags of CSAlignmentRecord from SAMRecord
      CSRecord.readUnmappedFlag = readSAM.getReadUnmappedFlag
      CSRecord.secondaryOrSupplementary = readSAM.isSecondaryOrSupplementary
      CSRecord.referenceIndex = readSAM.getReferenceIndex
      CSRecord.readNegativeStrandFlag = readSAM.getReadNegativeStrandFlag
      CSRecord.unclippedEnd = readSAM.getUnclippedEnd
      CSRecord.unclippedStart = readSAM.getUnclippedStart
      CSRecord.pairedFlag = readSAM.getReadPairedFlag
      CSRecord.firstOfPair = readSAM.getFirstOfPairFlag
      CSRecord.mateUnmappedFlag = readSAM.getMateUnmappedFlag
      CSRecord.mateReferenceIndex = readSAM.getMateReferenceIndex
      CSRecord.readName = readSAM.getReadName
      CSRecord.attribute = readSAM.getAttribute("RG").toString
      CSRecord.score = DuplicateScoringStrategy.computeDuplicateScore(readSAM, ScoringStrategy.SUM_OF_BASE_QUALITIES)
      CSRecord.libraryId = libraryIdGenerator.getLibraryId(readSAM)
      CSRecord.index = index

      CSRecord
    }

    /*
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
      }

      println("*** Finish building pairSort and fragSort! ***")
    }*/

    def buildReadEnds(header : SAMFileHeader, rec : CSAlignmentRecord, libraryIdGenerator : LibraryIdGenerator) : ReadEndsForMarkDuplicates = {
      // Build the ReadEnd for each read in ADAM
      val ends: ReadEndsForMarkDuplicates = new ReadEndsForMarkDuplicates()
      //val recSAM: SAMRecord = new AlignmentRecordConverter().convert(rec, new SAMFileHeaderWritable(header))

      ends.read1ReferenceIndex = Integer2int(rec.getReferenceIndex)
      if (rec.getReadNegativeStrandFlag) {
        ends.read1Coordinate = rec.getUnclippedEnd
        ends.orientation = ReadEnds.R
      }
      else {
        ends.read1Coordinate = rec.getUnclippedStart
        ends.orientation = ReadEnds.F
      }
      ends.read1IndexInFile = rec.getIndex
      ends.score = rec.getScore

      if (rec.getReadPairedFlag && !rec.getMateUnmappedFlag)
        ends.read2ReferenceIndex = rec.getMateReferenceIndex.asInstanceOf[Int]

      ends.libraryId = rec.getLibraryId

      if (opticalDuplicateFinder.addLocationInformation(rec.getReadName, ends)) {
        // calculate the RG number (nth in list)
        ends.readGroup = 0
        val rg: String = rec.getAttribute
        val readGroups: java.util.List[SAMReadGroupRecord] = header.getReadGroups

        if (rg != null && readGroups != null) {
          for (readGroup : SAMReadGroupRecord <- readGroups) {
            if (readGroup.getReadGroupId.equals(rg))
              break()
            else ends.readGroup += 1.toShort
          }
        }
      }

      ends
    }

    def findMate (tmp : java.util.ArrayList[CSAlignmentRecord], referenceIndex : Integer, readName : String) : CSAlignmentRecord = {
      // Find out the paired read that was already been processed but did not find their mate
      // Return the read or return null if no finding
      for (target : CSAlignmentRecord <- tmp) {
        if (target.getReferenceIndex == referenceIndex && target.getReadName == readName)
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
      if (OpticalDuplicateFinder.DEFAULT_READ_NAME_REGEX != null) {
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

    def writeToADAM(output : String, readsrdd : RDD[AlignmentRecord], sc : SparkContext) = {
      // Write results to ADAM file on HDFS
      println("*** Start to write reads back to ADAM file without duplicates! ***")

      // Transform duplicateIndexes into HashTable
      val dpIndexes = new java.util.Hashtable[Long, Long]()
      for (index : Long <- duplicateIndexes) {
        dpIndexes.put(index, index)
      }

      // Broadcast the duplicate indexes to nodes
      val broadcastDpIndexes = sc.broadcast(dpIndexes)

      val readADAMRDD = readsrdd.zipWithIndex().map{case (read : AlignmentRecord, index : Long) => {
        if (broadcastDpIndexes.value.contains(index)) {
          read.setDuplicateRead(true)
        } else read.setDuplicateRead(false)
        read
      }}

      /*var indexInFile : Long = 0
      var nextDuplicateIndex : Long = 0
      if (duplicateIndexes.hasNext) {
        nextDuplicateIndex = duplicateIndexes.next()
      } else {
        nextDuplicateIndex = -1
      }*/

      /*
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
      }*/

      // Use the filter function to get rid of those reads contains indexes in the duplicateIndexes
      val saveADAMRDDFilter = readADAMRDD.filter(read => read.getDuplicateRead.eq(false))
      val saveADAMRDD = new ADAMRDDFunctions(saveADAMRDDFilter)
      saveADAMRDD.adamParquetSave(output)
    }

    def main(args : Array[String]) = {
      val input = args(0)
      val output = args(1)
      //val headerinput = args(2)
      println("*** The directory of input ADAM file             : " + input)
      println("*** The directory of output ADAM file            : " + output)
      //println("*** The directory of input Fasta file for header : " + headerinput)

      val t0 = System.nanoTime : Double

      val conf = new SparkConf().setAppName("Mark Duplicate").setMaster("spark://10.0.1.2:7077")
      val sc = new SparkContext(conf)
      val ac = new ADAMContext(sc)
      val readsrdd: RDD[AlignmentRecord] = ac.loadAlignments(input)

      // Get the header for building (pair/frag)Sort
      val sd: SequenceDictionary = new ADAMSpecificRecordSequenceDictionaryRDDAggregator(readsrdd).adamGetSequenceDictionary(false)
      val rgd: RecordGroupDictionary = new AlignmentRecordRDDFunctions(readsrdd).adamGetReadGroupDictionary()
      val header: SAMFileHeader = new AlignmentRecordConverter().createSAMHeader(sd, rgd)
      val libraryIdGenerator = new LibraryIdGenerator(header)

      buildSortList(input, readsrdd, header, libraryIdGenerator)
      generateDupIndexes(libraryIdGenerator)
      writeToADAM(output, readsrdd, sc)

      val t1 = System.nanoTime() : Double

      println("*** Mark duplicate has been successfully done! ***")
      println("*** The total time for Mark Duplicate is : " + (t1-t0) / 1000000000.0 + "secs.")
    }
}
