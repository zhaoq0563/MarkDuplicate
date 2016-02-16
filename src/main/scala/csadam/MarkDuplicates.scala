///////////////////////////////////////////////////////////////
//***                                                     ***//
//*** Mark Duplicate by using ADAM store structure        ***//
//***                                                     ***//
//***	Created by Qi @ Oct. 2015                           ***//
//***                                                     ***//
//***	CS Department, UCLA                                 ***//
//***                                                     ***//
///////////////////////////////////////////////////////////////

package main.scala.csadam

import java.util.{Comparator, List, ArrayList}
import java.io.File
import cs.ucla.edu.bwaspark.datatype.{BNTSeqType, BWAIdxType}
import cs.ucla.edu.bwaspark.sam.SAMHeader
import htsjdk.samtools
import main.scala.csadam.util.{CSAlignmentQueuedMap, CSAlignmentRecord}
import htsjdk.samtools.util.Log
import htsjdk.samtools.DuplicateScoringStrategy.ScoringStrategy
import htsjdk.samtools._
import htsjdk.samtools.util.{SortingCollection, SortingLongCollection}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SAMFileHeaderWritable, SequenceDictionary}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDDFunctions
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMRDDFunctions, ADAMSequenceDictionaryRDDAggregator, ADAMSpecificRecordSequenceDictionaryRDDAggregator}
import org.bdgenomics.formats.avro.AlignmentRecord
import picard.sam.markduplicates.util._

import scala.util.control.Breaks.break

object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {

    var maxInMemory : Int = (Runtime.getRuntime.maxMemory() * 0.5 / ReadEndsForMarkDuplicates.SIZE_OF).toInt
    val tempDir = new File(System.getProperty("java.io.tmpdir"))
    var pairSort : SortingCollection[ReadEndsForMarkDuplicates] = SortingCollection.newInstance[ReadEndsForMarkDuplicates](classOf[ReadEndsForMarkDuplicates], new ReadEndsForMarkDuplicatesCodec(), new ReadEndsComparator, maxInMemory, tempDir)
    //var fragSort : SortingCollection[ReadEndsForMarkDuplicates] = SortingCollection.newInstance[ReadEndsForMarkDuplicates](classOf[ReadEndsForMarkDuplicates], new ReadEndsForMarkDuplicatesCodec(), new ReadEndsComparator, maxInMemory, tempDir)
    var fragSort = Array[CSAlignmentRecord]()
    var duplicateIndexes = new SortingLongCollection(100000)
    var numDuplicateIndices : Int = 0
    var LOG: Log = Log.getInstance(classOf[AbstractOpticalDuplicateFinderCommandLineProgram])
    val MAX_NUMBER_FOR_READ_MAP : Int = 8000
    this.opticalDuplicateFinder = new OpticalDuplicateFinder(OpticalDuplicateFinder.DEFAULT_READ_NAME_REGEX, 100, LOG)

    override def doWork() : Int = {
      val finish: Int = 0
      finish
    }

    // Transform the rdd in ADAM format to our self customized format to build pair/frag Sort list
    def buildSortList(input : String, readsrdd : RDD[AlignmentRecord], bns_bc : Broadcast[BNTSeqType], header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator, sc : SparkContext) = {
      println("*** Start to process the ADAM file to collect the information of all the reads into variables! ***\n")

//      readsrdd.saveAsTextFile("hdfs://cdsc0:9000/user/qzhao/temp")

//      println("\n*** Finish saving the original adam rdd! ***\n")
      println("The number of elements in the original RDD is: " + readsrdd.count() + "\n")

      println("*** Start zip! ***\n")

      // Map the ADAMrdd[AlignmentRecord] to CSrdd[CSRecord] with index

      val readRDDwithZip = readsrdd.zipWithIndex()

      println("*** Finish zip! ***\n")

      println("*** Start map! ***\n")

      val readCSIndexRDD = readRDDwithZip.map{case (read : AlignmentRecord, index : Long) => {
        val samHeader = new SAMHeader
        val samFileHeader = new SAMFileHeader
        val packageVersion = "v01"
        //val readGroupString = "@RG\tID:Sample_WGC033799D\tLB:Sample_WGC033799D\tSM:Sample_WGC033799D"
        val readGroupString = "@RG\tID:Sample_WGC033798D\tLB:Sample_WGC033798D\tSM:Sample_WGC033798D"
        samHeader.bwaGenSAMHeader(bns_bc.value, packageVersion, readGroupString, samFileHeader)
        val libraryIdGenerator = new LibraryIdGenerator(samFileHeader)

        val CSRecord : CSAlignmentRecord = buildCSAlignmentRecord(read, index, samFileHeader, libraryIdGenerator)

        CSRecord
      }}

      println("*** Finish mapping to CSAlignmentRecord! ***\n")

      val fragSortRDD = readCSIndexRDD.filter{read : CSAlignmentRecord => !(read.isSecondaryOrSupplementary)}.map{read : CSAlignmentRecord =>
        val samHeader = new SAMHeader
        val samFileHeader = new SAMFileHeader
        val packageVersion = "v01"
        //val readGroupString = "@RG\tID:Sample_WGC033799D\tLB:Sample_WGC033799D\tSM:Sample_WGC033799D"
        val readGroupString = "@RG\tID:Sample_WGC033798D\tLB:Sample_WGC033798D\tSM:Sample_WGC033798D"
        samHeader.bwaGenSAMHeader(bns_bc.value, packageVersion, readGroupString, samFileHeader)
        val libraryIdGenerator = new LibraryIdGenerator(samFileHeader)

        val fragmentEnd = buildFragSortRead(header, read, libraryIdGenerator)

        fragmentEnd
      }

      println("*** Start to collect data from CSAlignmentRecord RDD! ***\n")

//      readCSIndexRDD.foreach(x => {
//        if (x.isInstanceOf[CSAlignmentRecord] == true)
//          println("This is a fking CSAlignment Record!")
//      } )

      //readCSIndexRDD.saveAsTextFile("hdfs://cdsc0:9000/user/qzhao/temp")

//      println("\n*** Save as text successfully ***\n")

      fragSort = fragSortRDD.collect()
      println("The size of the fragSort is: " + fragSort.length + "\n")

      // Collect the data from CSrdd and iterate them to build frag/pair Sort
      val readArray = readCSIndexRDD.collect()
      println("The size of the read array is: " + readArray.length + "\n")
      //val readArray = readCSIndexRDD.take(10000)

      println("*** Finish collecting! ***\n")
//      // Load the header and library first
//      val bwaIdx = new BWAIdxType
//      val fastaLocalInputPath = "/space/scratch/ReferenceMetadata/human_g1k_v37.fasta"
//      bwaIdx.load(fastaLocalInputPath, 0)
//      val samHeader = new SAMHeader
//      val header = new SAMFileHeader
//      val packageVersion = "v01"
//      //val readGroupString = "@RG\tID:Sample_WGC033799D\tLB:Sample_WGC033799D\tSM:Sample_WGC033799D"
//      val readGroupString = "@RG\tID:Sample_WGC033798D\tLB:Sample_WGC033798D\tSM:Sample_WGC033798D"
//      samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion, readGroupString, header)
//      val libraryIdGenerator = new LibraryIdGenerator(header)


      //val tmp: java.util.ArrayList[CSAlignmentRecord] = new java.util.ArrayList[CSAlignmentRecord]
      val tmp : CSAlignmentQueuedMap[String, CSAlignmentRecord] = new CSAlignmentQueuedMap[String, CSAlignmentRecord](MAX_NUMBER_FOR_READ_MAP)

      println("*** Start to build pairSort and fragSort! ***\n")

      for (readCSRecord <- readArray) {
        if(readCSRecord.getIndex % 1000000 == 0) println("Process on: " + readCSRecord.getIndex)
        if (readCSRecord.getReadUnmappedFlag) {
          if (readCSRecord.getReferenceIndex == -1) {
            println("We are breaking from this point: " + readCSRecord.getIndex + " & the reference index is: " + readCSRecord.getReferenceIndex + "\n")
            break()
          }
        } else if (!readCSRecord.isSecondaryOrSupplementary) {
          val fragmentEnd = buildReadEnds(header, readCSRecord, libraryIdGenerator)
          //fragSort.add(fragmentEnd)

          if (readCSRecord.getReadPairedFlag && !readCSRecord.getMateUnmappedFlag) {
            val key = readCSRecord.getReferenceIndex.toString + readCSRecord.getReadName
            val checkPair = findMate(tmp, key)
            if (checkPair == null) {
              tmp.addItem((readCSRecord.getMateReferenceIndex + readCSRecord.getReadName), readCSRecord)
              //println("Temp size: " + tmp.length())
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

              pairedEnd.score = (pairedEnd.score + readCSRecord.getScore).toShort
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

      //fragSort.doneAdding()
      pairSort.doneAdding()

      println("*** Finish building pairSort and fragSort! ***\n")
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
      CSRecord.readName = readSAM.getReadName
      CSRecord.attribute = readSAM.getAttribute("RG").toString
      CSRecord.score = DuplicateScoringStrategy.computeDuplicateScore(readSAM, ScoringStrategy.SUM_OF_BASE_QUALITIES)
      CSRecord.libraryId = libraryIdGenerator.getLibraryId(readSAM)
      CSRecord.index = index

      if(readSAM.getReadPairedFlag && !readSAM.getMateUnmappedFlag)
        CSRecord.mateReferenceIndex = readSAM.getMateReferenceIndex

      CSRecord
    }

    def printCSAlignmentRecord(read : CSAlignmentRecord) = {
      println("\n*** The CSAlignment record has the following information:")
      println("Attribute: " + read.getAttribute)
      println("First of pair flag: " + read.getFirstOfPairFlag)
      println("Index: " + read.getIndex)
      println("Library Id: " + read.getLibraryId)
      println("Mate reference index: " + read.getMateReferenceIndex)
      println("Mate unmapped flag: " + read.getMateUnmappedFlag)
      println("Read name: " + read.getReadName)
      println("Read negative strand flag: " + read.getReadNegativeStrandFlag)
      println("Read paired flag: " + read.getReadPairedFlag)
      println("Read unmapped flag: " + read.getReadUnmappedFlag)
      println("Reference index: " + read.getReferenceIndex)
      println("Score: " + read.getScore)
      println("Unclipped end: " + read.getUnclippedEnd)
      println("Unclipped start: " + read.getUnclippedStart)
      println("Is secondary or supplementary: " + read.isSecondaryOrSupplementary)
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

      //println("\nThe name of Read is: " + rec.getReadName + "\n")
      //printCSAlignmentRecord(rec)

      //println("\n*** Test opticalDuplicateFinder: " + (this.opticalDuplicateFinder == null))

      if (this.opticalDuplicateFinder.addLocationInformation(rec.getReadName, ends)) {
        // calculate the RG number (nth in list)
        ends.readGroup = 0
        val rg: String = rec.getAttribute
        val readGroups : List[SAMReadGroupRecord] = header.getReadGroups
        val it = readGroups.iterator

        if (rg != null && readGroups != null) {
          while (it.hasNext) {
            if (!it.next.getReadGroupId.equals(rg))
              ends.readGroup = (ends.readGroup + 1).toShort
          }
        }
      }

      ends
    }

  def buildFragSortRead(header : SAMFileHeader, rec : CSAlignmentRecord, libraryIdGenerator : LibraryIdGenerator) : CSAlignmentRecord = {
    rec.read1ReferenceIndex = Integer2int(rec.getReferenceIndex)
    if (rec.getReadNegativeStrandFlag) {
      rec.read1Coordinate = rec.getUnclippedEnd
      rec.orientation = ReadEnds.R
    } else {
      rec.read1Coordinate = rec.getUnclippedStart
      rec.orientation = ReadEnds.F
    }
    rec.read1IndexInFile = rec.getIndex
    if (rec.getReadPairedFlag && !rec.getMateUnmappedFlag)
      rec.read2ReferenceIndex = rec.getMateReferenceIndex.asInstanceOf[Int]

    // Build one ReadEndsForMarkDuplicates for filling in optical duplicate location information
    val ends: ReadEndsForMarkDuplicates = new ReadEndsForMarkDuplicates()
    ends.read1ReferenceIndex = Integer2int(rec.getReferenceIndex)
    if (rec.getReadNegativeStrandFlag) {
      ends.read1Coordinate = rec.getUnclippedEnd
      ends.orientation = ReadEnds.R
    } else {
      ends.read1Coordinate = rec.getUnclippedStart
      ends.orientation = ReadEnds.F
    }
    ends.read1IndexInFile = rec.getIndex
    ends.score = rec.getScore
    if (rec.getReadPairedFlag && !rec.getMateUnmappedFlag)
      ends.read2ReferenceIndex = rec.getMateReferenceIndex.asInstanceOf[Int]
    ends.libraryId = rec.getLibraryId

    if (this.opticalDuplicateFinder.addLocationInformation(rec.getReadName, ends)) {
      // calculate the RG number (nth in list)
      rec.tile = ends.getTile
      rec.x = ends.x
      rec.y = ends.y
      rec.readGroup = 0
      val rg: String = rec.getAttribute
      val readGroups : List[SAMReadGroupRecord] = header.getReadGroups
      val it = readGroups.iterator

      if (rg != null && readGroups != null) {
        while (it.hasNext) {
          if (!it.next.getReadGroupId.equals(rg))
            rec.readGroup = (rec.readGroup + 1).toShort
        }
      }
    }

    rec
  }

    def findMate (tmp : CSAlignmentQueuedMap[String, CSAlignmentRecord], key : String) : CSAlignmentRecord = {
      // Find out the paired read that was already been processed but did not find their mate
      // Return the read or return null if no finding
      val mate = tmp.remove(key)
//      val it = tmp.iterator
//      while (it.hasNext) {
//        val tmpRecord : CSAlignmentRecord = it.next
//        if (tmpRecord.getReferenceIndex == referenceIndex && tmpRecord.getReadName == readName)
//          tmpRecord
//      }
      /*for (target : CSAlignmentRecord <- tmp) {
        if (target.getReferenceIndex == referenceIndex && target.getReadName == readName)
          target
      }*/
      mate
    }

    def generateDupIndexes(libraryIdGenerator : LibraryIdGenerator) = {
      // Generate the duplicate indexes for remove duplicate reads
      println("*** Start to generate duplicate indexes! ***\n")

      var firstOfNextChunk : ReadEndsForMarkDuplicates = null
      val nextChunk = new java.util.ArrayList[ReadEndsForMarkDuplicates](200)

      val itPairSort = pairSort.iterator
      while (itPairSort.hasNext) {
        val next : ReadEndsForMarkDuplicates = itPairSort.next
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
      var firstOfNextChunkF : CSAlignmentRecord = null
      val nextChunkF = new java.util.ArrayList[CSAlignmentRecord](200)

      val itFragSort = fragSort.iterator
      while (itFragSort.hasNext) {
        //val next : ReadEndsForMarkDuplicates = itFragSort.next
        val next : CSAlignmentRecord = itFragSort.next
        if (firstOfNextChunk != null && areComparableForDuplicatesF(firstOfNextChunkF, next)) {
          nextChunkF.add(next)
          containsPairs = containsPairs || next.isPaired
          containsFrags = containsFrags || !next.isPaired
        } else {
          if (nextChunkF.size() > 1 && containsFrags) markDuplicateFragmentsF(nextChunkF, containsPairs)
          nextChunkF.clear()
          nextChunkF.add(next)
          firstOfNextChunkF = next
          containsPairs = next.isPaired
          containsFrags = !next.isPaired
        }
      }
      markDuplicateFragmentsF(nextChunkF, containsPairs)
      //fragSort.cleanup()
      fragSort = null

      println("*** Finish generating duplicate indexes! ***\n")
      println("*** [REAL]The number of duplicate is: " + this.numDuplicateIndices + "\n")
      duplicateIndexes.doneAddingStartIteration()
    }

    def areComparableForDuplicates(lhs : ReadEndsForMarkDuplicates, rhs : ReadEndsForMarkDuplicates, compareRead2 : Boolean) : Boolean = {
      var retval: Boolean = (lhs.libraryId == rhs.libraryId) && (lhs.read1ReferenceIndex == rhs.read1ReferenceIndex) && (lhs.read1Coordinate == rhs.read1Coordinate) && (lhs.orientation == rhs.orientation)

      if (retval && compareRead2) {
        retval = (lhs.read2ReferenceIndex == rhs.read2ReferenceIndex) && (lhs.read2Coordinate == rhs.read2Coordinate)
      }

      retval
    }

  def areComparableForDuplicatesF(lhs : CSAlignmentRecord, rhs : CSAlignmentRecord) : Boolean = {
    var retval: Boolean = (lhs.libraryId == rhs.libraryId) && (lhs.read1ReferenceIndex == rhs.read1ReferenceIndex) && (lhs.read1Coordinate == rhs.read1Coordinate) && (lhs.orientation == rhs.orientation)

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
      val it1st = list.iterator
      while (it1st.hasNext) {
        val end = it1st.next
        if (end.score > maxScore || best == null) {
          maxScore = end.score
          best = end
        }
      }
      /*for (end : ReadEndsForMarkDuplicates <- list) {
        if (end.score > maxScore || best == null) {
          maxScore = end.score
          best = end
        }
      }*/

      val it2nd = list.iterator
      while (it2nd.hasNext) {
        val end = it2nd.next
        if (end != best) {
          addIndexAsDuplicate(end.read1IndexInFile)
          addIndexAsDuplicate(end.read2IndexInFile)
        }
      }
      /*for (end : ReadEndsForMarkDuplicates <- list) {
        if (end != best) {
          addIndexAsDuplicate(end.read1IndexInFile)
          addIndexAsDuplicate(end.read2IndexInFile)
        }
      }*/

      // Null for libraryIdGenerator, need to figure out how to get SAM/BAM header
      if (OpticalDuplicateFinder.DEFAULT_READ_NAME_REGEX != null) {
        AbstractMarkDuplicatesCommandLineProgram.trackOpticalDuplicates(list, opticalDuplicateFinder, libraryIdGenerator)
      }
    }

    def markDuplicateFragments(list : java.util.ArrayList[ReadEndsForMarkDuplicates], containsPairs : Boolean) = {
      if (containsPairs) {
        val it1st = list.iterator
        while (it1st.hasNext) {
          val end = it1st.next
          if (!end.isPaired) addIndexAsDuplicate(end.read1IndexInFile)
        }
        /*for (end : ReadEndsForMarkDuplicates <- list) {
          if (!end.isPaired) addIndexAsDuplicate(end.read1IndexInFile)
        }*/
      } else {
        var maxScore : Short = 0
        var best : ReadEndsForMarkDuplicates = null
        val it2nd = list.iterator
        while (it2nd.hasNext) {
          val end = it2nd.next
          if (end.score > maxScore || best == null) {
            maxScore = end.score
            best = end
          }
        }
        /*for (end : ReadEndsForMarkDuplicates <- list) {
          if (end.score > maxScore || best == null) {
            maxScore = end.score
            best = end
          }
        }*/

        val it3rd = list.iterator
        while (it3rd.hasNext) {
          val end = it3rd.next
          if (end != best) {
            addIndexAsDuplicate(end.read1IndexInFile)
          }
        }
        /*for (end : ReadEndsForMarkDuplicates <- list) {
          if (end != best) {
            addIndexAsDuplicate(end.read1IndexInFile)
          }
        }*/
      }
    }

  def markDuplicateFragmentsF(list : java.util.ArrayList[CSAlignmentRecord], containsPairs : Boolean) = {
    if (containsPairs) {
      val it1st = list.iterator
      while (it1st.hasNext) {
        val end = it1st.next
        if (!end.isPaired) addIndexAsDuplicate(end.read1IndexInFile)
      }
    } else {
      var maxScore : Short = 0
      var best : CSAlignmentRecord = null
      val it2nd = list.iterator
      while (it2nd.hasNext) {
        val end = it2nd.next
        if (end.score > maxScore || best == null) {
          maxScore = end.score
          best = end
        }
      }
      val it3rd = list.iterator
      while (it3rd.hasNext) {
        val end = it3rd.next
        if (end != best) {
          addIndexAsDuplicate(end.read1IndexInFile)
        }
      }
    }
  }

    def writeToADAM(output : String, readsrdd : RDD[AlignmentRecord], sc : SparkContext) = {
      // Write results to ADAM file on HDFS
      println("*** Start to write reads back to ADAM file without duplicates! ***\n")

      // Transform duplicateIndexes into HashTable
      val dpIndexes = new java.util.Hashtable[Long, Long]()
      var dpCounter = 0
      while (duplicateIndexes.hasNext) {
        val value = duplicateIndexes.next
        dpIndexes.put(value, value)
        dpCounter += 1
      }

      println("*** [COUNT]The number of duplicate is: " + dpCounter + " !***\n")
      /*for (index <- duplicateIndexes) {
        dpIndexes.put(index, index)
      }*/

      // Broadcast the duplicate indexes to nodes
      val broadcastDpIndexes = sc.broadcast(dpIndexes)

      val readADAMRDD = readsrdd.zipWithIndex().map{case (read : AlignmentRecord, index : Long) => {
        if (broadcastDpIndexes.value.contains(index)) {
          read.setDuplicateRead(boolean2Boolean(true))
        } else read.setDuplicateRead(boolean2Boolean(false))
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
      val saveADAMRDDFilter = readADAMRDD.filter(read  => read.getDuplicateRead.equals(false))
      val saveADAMRDD = new ADAMRDDFunctions(saveADAMRDDFilter)
      println("*** The number of reads after mark duplicate: " + saveADAMRDDFilter.count() + "\n")
      saveADAMRDD.adamParquetSave(output)
    }

    class ReadEndsComparator extends Comparator[ReadEndsForMarkDuplicates] {

      def compare(lhs: ReadEndsForMarkDuplicates, rhs: ReadEndsForMarkDuplicates): Int = {
        var retval : Int = lhs.libraryId - rhs.libraryId
        if(retval == 0) {
          retval = lhs.read1ReferenceIndex - rhs.read1ReferenceIndex
        }

        if(retval == 0) {
          retval = lhs.read1Coordinate - rhs.read1Coordinate
        }

        if(retval == 0) {
          retval = lhs.orientation - rhs.orientation
        }

        if(retval == 0) {
          retval = lhs.read2ReferenceIndex - rhs.read2ReferenceIndex
        }

        if(retval == 0) {
          retval = lhs.read2Coordinate - rhs.read2Coordinate
        }

        if(retval == 0) {
          retval = (lhs.read1IndexInFile - rhs.read1IndexInFile).toInt
        }

        if(retval == 0) {
          retval = (lhs.read2IndexInFile - rhs.read2IndexInFile).toInt
        }

        retval
      }
    }

    def main(args : Array[String]) = {
      val input = args(0)
      val output = args(1)
      //val headerinput = args(2)
      println("*** The directory of input ADAM file             : " + input + "\n")
      println("*** The directory of output ADAM file            : " + output + "\n")
      //println("*** The directory of input Fasta file for header : " + headerinput)

      val t0 = System.nanoTime : Double

      val conf = new SparkConf().setAppName("Mark Duplicate").setMaster("spark://10.0.1.2:7077").set("spark.driver.maxResultSize", "100G").set("spark.network.timeout", "500s").set("spark.cores.max", "500")
      val sc = new SparkContext(conf)
      val ac = new ADAMContext(sc)
      val readsRDD: RDD[AlignmentRecord] = ac.loadAlignments(input)

      println("*** The alignments are loaded successfully! ***\n")
//      val testsave = new ADAMRDDFunctions(readsRDD)
//      testsave.adamParquetSave("hdfs://cdsc0:9000/user/qzhao/data/test.adam")
//      println("*** Test save successfully! ***")

      //val sd: SequenceDictionary = new ADAMSpecificRecordSequenceDictionaryRDDAggregator(readsRDD).adamGetSequenceDictionary(false)
      //val rgd: RecordGroupDictionary = new AlignmentRecordRDDFunctions(readsRDD).adamGetReadGroupDictionary()
      //val header: SAMFileHeader = new AlignmentRecordConverter().createSAMHeader(sd, rgd)

      // Load bwaIdx in master node
      val bwaIdx = new BWAIdxType
      val fastaLocalInputPath = "/space/scratch/ReferenceMetadata/human_g1k_v37.fasta"
      bwaIdx.load(fastaLocalInputPath, 0)

      // Boardcast the bns
      val bns_bc : Broadcast[BNTSeqType] = sc.broadcast(bwaIdx.bns)

      val samHeader = new SAMHeader
      val samFileHeader = new SAMFileHeader
      val packageVersion = "v01"
      //val readGroupString = "@RG\tID:Sample_WGC033799D\tLB:Sample_WGC033799D\tSM:Sample_WGC033799D"
      val readGroupString = "@RG\tID:Sample_WGC033798D\tLB:Sample_WGC033798D\tSM:Sample_WGC033798D"
      samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion, readGroupString, samFileHeader)
      val libraryIdGenerator = new LibraryIdGenerator(samFileHeader)

//      println("*** Ready to collect alignment record from readsRDD! ***")
//      val readArray = readsRDD.collect()
//      println("*** Collecting successfully! ***")

      buildSortList(input, readsRDD, bns_bc, samFileHeader, libraryIdGenerator, sc)
      // @ Need to be done
      generateDupIndexes(libraryIdGenerator) // Passing bwaIdx_bc and access it on workers instead of creating a new one here
      writeToADAM(output, readsRDD, sc)

      // @ Need to be done
      val numOpticalDuplicates = libraryIdGenerator.getOpticalDuplicatesByLibraryIdMap.getSumOfValues.toLong
      println("*** The number of optical duplicates are : " + numOpticalDuplicates + " !***\n")

      val t1 = System.nanoTime() : Double

      println("*** Mark duplicate has been successfully done! ***\n")
      println("*** The total time for Mark Duplicate is : " + (t1-t0) / 1000000000.0 + " secs.\n")
    }
}
