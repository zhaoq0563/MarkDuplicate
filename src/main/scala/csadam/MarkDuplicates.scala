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
import main.scala.csadam.util.{CSAlignmentQueuedMap, CSAlignmentRecord}
import htsjdk.samtools.util.Log
import htsjdk.samtools.DuplicateScoringStrategy.ScoringStrategy
import htsjdk.samtools._
import htsjdk.samtools.util.{SortingCollection, SortingLongCollection}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{RecordGroupDictionary, SAMFileHeaderWritable, SequenceDictionary}
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMRDDFunctions, ADAMSequenceDictionaryRDDAggregator, ADAMSpecificRecordSequenceDictionaryRDDAggregator}
import org.bdgenomics.formats.avro.AlignmentRecord
import picard.sam.markduplicates.util._
import org.bdgenomics.adam.rdd.ADAMContext._

import scala.collection.mutable

object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {

    var maxInMemory : Int = (Runtime.getRuntime.maxMemory() * 0.5 / ReadEndsForMarkDuplicates.SIZE_OF).toInt
    val tempDir = new File(System.getProperty("java.io.tmpdir"))
    var pairSort : SortingCollection[ReadEndsForMarkDuplicates] = SortingCollection.newInstance[ReadEndsForMarkDuplicates](classOf[ReadEndsForMarkDuplicates], new ReadEndsForMarkDuplicatesCodec(), new ReadEndsComparator, maxInMemory, tempDir)
    var fragSort : SortingCollection[ReadEndsForMarkDuplicates] = SortingCollection.newInstance[ReadEndsForMarkDuplicates](classOf[ReadEndsForMarkDuplicates], new ReadEndsForMarkDuplicatesCodec(), new ReadEndsComparator, maxInMemory, tempDir)
    //var fragSort = Array[CSAlignmentRecord]()
    var duplicateIndexes : SortingLongCollection = _
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

      println("*** Start zip! ***\n")

      // Map the ADAMrdd[AlignmentRecord] to CSrdd[CSRecord] with index

      val readRDDwithZip = readsrdd.zipWithIndex()

      println("*** Finish zip! ***\n")

      println("*** Start map to CSAlignment! ***\n")

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
      //.filter{read : CSAlignmentRecord => (!(read.getReadUnmappedFlag) && !(read.isSecondaryOrSupplementary))}

//      val readArray = readCSIndexRDD.collect()
//
//      var index = 0
//      var continue = 1
//      val tmp = new DiskBasedReadEndsForMarkDuplicatesMap(MAX_NUMBER_FOR_READ_MAP)
//      val it = readArray.iterator
//      while(it.hasNext && continue == 1) {
//        var readCSRecord = it.next()
//
//        if (readCSRecord.getReadUnmappedFlag) {
//          if (readCSRecord.getReferenceIndex == -1) {
//            continue = 0
//          }
//        }
//        else if (!readCSRecord.isSecondaryOrSupplementary) {
//          val fragmentEnd = buildReadEnds(header, readCSRecord, libraryIdGenerator)
//          this.fragSort.add(fragmentEnd)
//
//          if (readCSRecord.getReadPairedFlag && !readCSRecord.getMateUnmappedFlag) {
//            val key = readCSRecord.getReadGroupID + ":" + readCSRecord.getReadName
//            var checkPair = tmp.remove(Integer2int(readCSRecord.getReferenceIndex), key)
//            if (checkPair == null) {
//              checkPair = buildReadEnds(header, readCSRecord, libraryIdGenerator)
//              tmp.put(checkPair.read2ReferenceIndex, key, checkPair)
//            } else {
//              val sequence: Int = readCSRecord.read1ReferenceIndex
//              val coordinate: Int = readCSRecord.read1Coordinate
//
//              if (readCSRecord.getFirstOfPairFlag) {
//                checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
//              } else {
//                checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
//              }
//
//              if (sequence > checkPair.read1ReferenceIndex || (sequence == checkPair.read1ReferenceIndex && coordinate >= checkPair.read1Coordinate)) {
//                checkPair.read2ReferenceIndex = sequence
//                checkPair.read2Coordinate = coordinate
//                checkPair.read2IndexInFile = readCSRecord.getIndex
//                checkPair.orientation = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
//              } else {
//                checkPair.read2ReferenceIndex = checkPair.read1ReferenceIndex
//                checkPair.read2Coordinate = checkPair.read1Coordinate
//                checkPair.read2IndexInFile = checkPair.read1IndexInFile
//                checkPair.read1ReferenceIndex = sequence
//                checkPair.read1Coordinate = coordinate
//                checkPair.read1IndexInFile = readCSRecord.getIndex
//                checkPair.orientation = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
//              }
//
//              checkPair.score = (checkPair.score + readCSRecord.getScore).toShort
//              this.pairSort.add(checkPair)
//            }
//          }
//        }
//        index += 1
//      }

      //val totalReads = readCSIndexRDD.count()

      //println("*** There are totally " + totalReads + " reads for marking duplicates! ***\n")

      var end = 0
      var count = 0
      var index = 0
      var totalTake = 0
      val partSize = 200000000
      //val iteration = totalReads/partSize + 1
      val tmp = new DiskBasedReadEndsForMarkDuplicatesMap(MAX_NUMBER_FOR_READ_MAP)

      println("*** Start to build fragSort and pairSort for every 200 million reads! ***\n")

      while(end == 0) {
        // Collect 10 million each iteration to build fragSort and PairSort
        // 1, Filter out those already been built and convert reads to CSAlignmentRecord
        val readIterationRDD = readCSIndexRDD.filter{read : CSAlignmentRecord => {read.getIndex >= (count * partSize)}}

        // 2, Collect back the first partSize reads
        val readArray = readCSIndexRDD.take(totalTake)
        totalTake = readArray.length
        if (totalTake != partSize) {
          end = 1
        }

        println("*** Process on " + (count * partSize) + " to "  + (count * partSize + totalTake) + " reads! ***\n")

        // 3, Build the fragSort and PairSort
        val it = readArray.iterator
        var continue = 1
        while(it.hasNext && continue == 1) {
          var readCSRecord = it.next()

          if (readCSRecord.getReadUnmappedFlag) {
            if (readCSRecord.getReferenceIndex == -1) {
              continue = 0
              index -= 1
            }
          }
          else if (!readCSRecord.isSecondaryOrSupplementary) {
            val fragmentEnd = buildReadEnds(header, readCSRecord, libraryIdGenerator)
            this.fragSort.add(fragmentEnd)

            if (readCSRecord.getReadPairedFlag && !readCSRecord.getMateUnmappedFlag) {
              val key = readCSRecord.getReadGroupID + ":" + readCSRecord.getReadName
              var checkPair = tmp.remove(Integer2int(readCSRecord.getReferenceIndex), key)
              if (checkPair == null) {
                checkPair = buildReadEnds(header, readCSRecord, libraryIdGenerator)
                tmp.put(checkPair.read2ReferenceIndex, key, checkPair)
              } else {
                val sequence: Int = readCSRecord.read1ReferenceIndex
                val coordinate: Int = readCSRecord.read1Coordinate

                if (readCSRecord.getFirstOfPairFlag) {
                  checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
                } else {
                  checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
                }

                if (sequence > checkPair.read1ReferenceIndex || (sequence == checkPair.read1ReferenceIndex && coordinate >= checkPair.read1Coordinate)) {
                  checkPair.read2ReferenceIndex = sequence
                  checkPair.read2Coordinate = coordinate
                  checkPair.read2IndexInFile = readCSRecord.getIndex
                  checkPair.orientation = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
                } else {
                  checkPair.read2ReferenceIndex = checkPair.read1ReferenceIndex
                  checkPair.read2Coordinate = checkPair.read1Coordinate
                  checkPair.read2IndexInFile = checkPair.read1IndexInFile
                  checkPair.read1ReferenceIndex = sequence
                  checkPair.read1Coordinate = coordinate
                  checkPair.read1IndexInFile = readCSRecord.getIndex
                  checkPair.orientation = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
                }

                checkPair.score = (checkPair.score + readCSRecord.getScore).toShort
                this.pairSort.add(checkPair)
              }
            }
          }
          index += 1
        }
//        while(it.hasNext) {
//          var readCSRecord = it.next()
//          val fragmentEnd = buildReadEnds(header, readCSRecord, libraryIdGenerator)
//          this.fragSort.add(fragmentEnd)
//
//          if (readCSRecord.getReadPairedFlag && !readCSRecord.getMateUnmappedFlag) {
//            val key = readCSRecord.getReadGroupID + ":" + readCSRecord.getReadName
//            var checkPair = tmp.remove(Integer2int(readCSRecord.getReferenceIndex), key)
//            if (checkPair == null) {
//              checkPair = buildReadEnds(header, readCSRecord, libraryIdGenerator)
//              tmp.put(checkPair.read2ReferenceIndex, key, checkPair)
//            } else {
//              val sequence: Int = readCSRecord.read1ReferenceIndex
//              val coordinate: Int = readCSRecord.read1Coordinate
//
//              if (readCSRecord.getFirstOfPairFlag) {
//                checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
//              } else {
//                checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
//              }
//
//              if (sequence > checkPair.read1ReferenceIndex || (sequence == checkPair.read1ReferenceIndex && coordinate >= checkPair.read1Coordinate)) {
//                checkPair.read2ReferenceIndex = sequence
//                checkPair.read2Coordinate = coordinate
//                checkPair.read2IndexInFile = readCSRecord.getIndex
//                checkPair.orientation = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
//              } else {
//                checkPair.read2ReferenceIndex = checkPair.read1ReferenceIndex
//                checkPair.read2Coordinate = checkPair.read1Coordinate
//                checkPair.read2IndexInFile = checkPair.read1IndexInFile
//                checkPair.read1ReferenceIndex = sequence
//                checkPair.read1Coordinate = coordinate
//                checkPair.read1IndexInFile = readCSRecord.getIndex
//                checkPair.orientation = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
//              }
//
//              checkPair.score = (checkPair.score + readCSRecord.getScore).toShort
//              this.pairSort.add(checkPair)
//            }
//          }
//          index += 1
//        }
        count += 1
      }

//      val readCSIndexRDD = readRDDwithZip.map{case (read : AlignmentRecord, index : Long) => {
//        val samHeader = new SAMHeader
//        val samFileHeader = new SAMFileHeader
//        val packageVersion = "v01"
//        //val readGroupString = "@RG\tID:Sample_WGC033799D\tLB:Sample_WGC033799D\tSM:Sample_WGC033799D"
//        val readGroupString = "@RG\tID:Sample_WGC033798D\tLB:Sample_WGC033798D\tSM:Sample_WGC033798D"
//        samHeader.bwaGenSAMHeader(bns_bc.value, packageVersion, readGroupString, samFileHeader)
//        val libraryIdGenerator = new LibraryIdGenerator(samFileHeader)
//
//        val CSRecord : CSAlignmentRecord = buildCSAlignmentRecord(read, index, samFileHeader, libraryIdGenerator)
//
//        CSRecord
//      }}
//
//      //println("*** The number of elements in the CSAlignment RDD is: " + readCSIndexRDD.count() + "\n")
//
//      println("*** Finish mapping to CSAlignmentRecord! ***\n")
//
//      println("*** Start map to fragSort! ***\n")
//
//      val fragSortRDD = readCSIndexRDD.filter{read : CSAlignmentRecord => (!(read.getReadUnmappedFlag) && !(read.isSecondaryOrSupplementary))}.map{read : CSAlignmentRecord =>
//        val samHeader = new SAMHeader
//        val samFileHeader = new SAMFileHeader
//        val packageVersion = "v01"
//        //val readGroupString = "@RG\tID:Sample_WGC033799D\tLB:Sample_WGC033799D\tSM:Sample_WGC033799D"
//        val readGroupString = "@RG\tID:Sample_WGC033798D\tLB:Sample_WGC033798D\tSM:Sample_WGC033798D"
//        samHeader.bwaGenSAMHeader(bns_bc.value, packageVersion, readGroupString, samFileHeader)
//        val libraryIdGenerator = new LibraryIdGenerator(samFileHeader)
//
//        val fragmentEnd = buildFragSortRead(header, read, libraryIdGenerator)
//
//        fragmentEnd
//      }

      //println("*** The number of elements in the fragSort RDD is: " + fragSortRDD.count() + "\n")

//      println("*** Finish mapping to fragSort! ***\n")
//
//      println("*** Start map to pairSort! ***\n")

//      val hashPairRDD = fragSortRDD.filter{read : CSAlignmentRecord => (read.getReadPairedFlag && !read.getMateUnmappedFlag)}.map{read : CSAlignmentRecord =>
//        val hashString =
//
//      }

//      println("*** Finish mapping to pairSort! ***\n")
//
//      println("*** Start to collect data from fragSort RDD! ***\n")

//      fragSort = fragSortRDD.collect()

//      println("*** The size of the fragSort is: " + fragSort.length + "\n")

      // Collect the data from CSrdd and iterate them to build frag/pair Sort
      //val readArray = readCSIndexRDD.collect()
      //println("The size of the read array is: " + readArray.length + "\n")
      //val readArray = readCSIndexRDD.take(10000)

//      println("*** Finish collecting! ***\n")

      //val tmp: java.util.ArrayList[CSAlignmentRecord] = new java.util.ArrayList[CSAlignmentRecord]
      //val tmp : CSAlignmentQueuedMap[String, CSAlignmentRecord] = new CSAlignmentQueuedMap[String, CSAlignmentRecord](MAX_NUMBER_FOR_READ_MAP)
//      val tmp = new DiskBasedReadEndsForMarkDuplicatesMap(MAX_NUMBER_FOR_READ_MAP)

//      println("*** Start to build pairSort and fragSort! ***\n")

//      val iterator = fragSort.iterator
//      var index = 0
//      while (iterator.hasNext) {
//        var readCSRecord = iterator.next()
//        //for (readCSRecord <- fragSort) {
//        //        if (readCSRecord.getReadUnmappedFlag) {
//        //          if (readCSRecord.getReferenceIndex == -1) {
//        //            println("We are breaking from this point: " + readCSRecord.getIndex + " & the reference index is: " + readCSRecord.getReferenceIndex + "\n")
//        //            break()
//        //          }
//        //        } else if (!readCSRecord.isSecondaryOrSupplementary) {
//        //val fragmentEnd = buildReadEnds(header, readCSRecord, libraryIdGenerator)
//        //fragSort.add(fragmentEnd)
//
//        if (readCSRecord.getReadPairedFlag && !readCSRecord.getMateUnmappedFlag) {
//          //val key = readCSRecord.getReferenceIndex.toString + readCSRecord.getReadName
//          val key = readCSRecord.getReadGroupID + ":" + readCSRecord.getReadName
//          //val checkPair = findMate(tmp, key)
//          var checkPair = tmp.remove(Integer2int(readCSRecord.getReferenceIndex), key)
//          if (checkPair == null) {
//            checkPair = buildReadEnds(header, readCSRecord, libraryIdGenerator)
//            //tmp.addItem((readCSRecord.getMateReferenceIndex + readCSRecord.getReadName), readCSRecord)
//            tmp.put(checkPair.read2ReferenceIndex, key, checkPair)
//          } else {
//            val sequence: Int = readCSRecord.read1ReferenceIndex
//            val coordinate: Int = readCSRecord.read1Coordinate
//
//            if (readCSRecord.getFirstOfPairFlag) {
//              checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
//            } else {
//              checkPair.orientationForOpticalDuplicates = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
//            }
//
//            if (sequence > checkPair.read1ReferenceIndex || (sequence == checkPair.read1ReferenceIndex && coordinate >= checkPair.read1Coordinate)) {
//              checkPair.read2ReferenceIndex = sequence
//              checkPair.read2Coordinate = coordinate
//              checkPair.read2IndexInFile = readCSRecord.getIndex
//              checkPair.orientation = ReadEnds.getOrientationByte(checkPair.orientation == ReadEnds.R, readCSRecord.getReadNegativeStrandFlag)
//            } else {
//              checkPair.read2ReferenceIndex = checkPair.read1ReferenceIndex
//              checkPair.read2Coordinate = checkPair.read1Coordinate
//              checkPair.read2IndexInFile = checkPair.read1IndexInFile
//              checkPair.read1ReferenceIndex = sequence
//              checkPair.read1Coordinate = coordinate
//              checkPair.read1IndexInFile = readCSRecord.getIndex
//              checkPair.orientation = ReadEnds.getOrientationByte(readCSRecord.getReadNegativeStrandFlag, checkPair.orientation == ReadEnds.R)
//            }
//
//            checkPair.score = (checkPair.score + readCSRecord.getScore).toShort
//            pairSort.add(checkPair)
//          }
//        }
//        index += 1
//        //if (index % 1000000 == 0) println("Process on: " + index)
//        //}
//      }

      println("*** Read " + index + " records. " + tmp.size() + " pairs never matched. ***\n")

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

      println("*** Finish building pairSort and fragSort! ***\n")
    }

    def buildCSAlignmentRecord(read : AlignmentRecord, index : Long, header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator) : CSAlignmentRecord = {
      // Build one single CSAlignmentRecord for CSrdd with index
      val CSRecord : CSAlignmentRecord = new CSAlignmentRecord()
      val readSAM : SAMRecord = convertADAMtoSam(read, new SAMFileHeaderWritable(header))

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
      CSRecord.readgroupid = readSAM.getAttribute(ReservedTagConstants.READ_GROUP_ID).toString

      if(readSAM.getReadPairedFlag && !readSAM.getMateUnmappedFlag)
        CSRecord.mateReferenceIndex = readSAM.getMateReferenceIndex

      CSRecord
    }

    def convertADAMtoSam(adamRecord: AlignmentRecord, header: SAMFileHeaderWritable): SAMRecord = {
      // get read group dictionary from header
      val rgDict = header.header.getSequenceDictionary

      // attach header
      val builder: SAMRecord = new SAMRecord(header.header)

      // set canonically necessary fields
      builder.setReadName(adamRecord.getReadName.toString)
      builder.setReadString(adamRecord.getSequence)
      adamRecord.getQual match {
        case null      => builder.setBaseQualityString("*")
        case s: String => builder.setBaseQualityString(s)
      }

      // set read group flags
      Option(adamRecord.getRecordGroupName)
        .map(_.toString)
        .map(rgDict.getSequenceIndex)
        .foreach(v => builder.setAttribute("RG", v.toString))
      Option(adamRecord.getRecordGroupLibrary)
        .foreach(v => builder.setAttribute("LB", v.toString))
      Option(adamRecord.getRecordGroupPlatformUnit)
        .foreach(v => builder.setAttribute("PU", v.toString))

      // set the reference name, and alignment position, for mate
      Option(adamRecord.getMateContig)
        .map(_.getContigName)
        .map(_.toString)
        .foreach(builder.setMateReferenceName)
      Option(adamRecord.getMateAlignmentStart)
        .foreach(s => builder.setMateAlignmentStart(s.toInt + 1))

      // set template length
      Option(adamRecord.getInferredInsertSize)
        .foreach(s => builder.setInferredInsertSize(s.toInt))

      // set flags
      Option(adamRecord.getReadPaired).foreach(p => {
        builder.setReadPairedFlag(p.booleanValue)

        // only set flags if read is paired
        if (p) {
          Option(adamRecord.getMateNegativeStrand)
            .foreach(v => builder.setMateNegativeStrandFlag(v.booleanValue))
          Option(adamRecord.getMateMapped)
            .foreach(v => builder.setMateUnmappedFlag(!v.booleanValue))
          Option(adamRecord.getProperPair)
            .foreach(v => builder.setProperPairFlag(v.booleanValue))
          Option(adamRecord.getReadNum == 0)
            .foreach(v => builder.setFirstOfPairFlag(v.booleanValue))
          Option(adamRecord.getReadNum == 1)
            .foreach(v => builder.setSecondOfPairFlag(v.booleanValue))
        }
      })
      Option(adamRecord.getDuplicateRead)
        .foreach(v => builder.setDuplicateReadFlag(v.booleanValue))
      Option(adamRecord.getReadMapped)
        .foreach(m => {
          builder.setReadUnmappedFlag(!m.booleanValue)

          // only set alignment flags if read is aligned
          if (m) {
            // if we are aligned, we must have a reference
            assert(adamRecord.getContig != null, "Cannot have null contig if aligned.")
            builder.setReferenceName(adamRecord.getContig.getContigName)

            // set the cigar, if provided
            Option(adamRecord.getCigar).map(_.toString).foreach(builder.setCigarString)
            // set the old cigar, if provided
            Option(adamRecord.getOldCigar).map(_.toString).foreach(v => builder.setAttribute("OC", v))
            // set mapping flags
            Option(adamRecord.getReadNegativeStrand)
              .foreach(v => builder.setReadNegativeStrandFlag(v.booleanValue))
            Option(adamRecord.getPrimaryAlignment)
              .foreach(v => builder.setNotPrimaryAlignmentFlag(!v.booleanValue))
            Option(adamRecord.getSupplementaryAlignment)
              .foreach(v => builder.setSupplementaryAlignmentFlag(v.booleanValue))
            Option(adamRecord.getStart)
              .foreach(s => builder.setAlignmentStart(s.toInt + 1))
            Option(adamRecord.getOldPosition)
              .foreach(s => builder.setAttribute("OP", s.toInt + 1))
            Option(adamRecord.getMapq).foreach(v => builder.setMappingQuality(v))
          } else {
            // mapping quality must be 0 if read is unmapped
            builder.setMappingQuality(0)
          }
        })
      Option(adamRecord.getFailedVendorQualityChecks)
        .foreach(v => builder.setReadFailsVendorQualityCheckFlag(v.booleanValue))
      Option(adamRecord.getMismatchingPositions)
        .map(_.toString)
        .foreach(builder.setAttribute("MD", _))

      // add all other tags
//      if (adamRecord.getAttributes != null) {
//        val mp = RichAlignmentRecord(adamRecord).tags
//        mp.foreach(a => {
//          builder.setAttribute(a.tag, a.value)
//        })
//      }

      // return sam record
      builder
    }

//    def buildCSAlignmentRecord(read : AlignmentRecord, index : Long, header : SAMFileHeader, libraryIdGenerator : LibraryIdGenerator) : CSAlignmentRecord = {
//      // Build one single CSAlignmentRecord for CSrdd with index
//      val CSRecord : CSAlignmentRecord = new CSAlignmentRecord()
//      val readSAM : SAMRecord = new AlignmentRecordConverter().convert(read, new SAMFileHeaderWritable(header))
//
//      // Assign the flags of CSAlignmentRecord from SAMRecord
//      CSRecord.readUnmappedFlag = readSAM.getReadUnmappedFlag
//      CSRecord.secondaryOrSupplementary = readSAM.isSecondaryOrSupplementary
//      CSRecord.referenceIndex = readSAM.getReferenceIndex
//      CSRecord.readNegativeStrandFlag = readSAM.getReadNegativeStrandFlag
//      CSRecord.unclippedEnd = readSAM.getUnclippedEnd
//      CSRecord.unclippedStart = readSAM.getUnclippedStart
//      CSRecord.pairedFlag = readSAM.getReadPairedFlag
//      CSRecord.firstOfPair = readSAM.getFirstOfPairFlag
//      CSRecord.mateUnmappedFlag = readSAM.getMateUnmappedFlag
//      CSRecord.readName = readSAM.getReadName
//      CSRecord.attribute = readSAM.getAttribute("RG").toString
//      CSRecord.score = DuplicateScoringStrategy.computeDuplicateScore(readSAM, ScoringStrategy.SUM_OF_BASE_QUALITIES)
//      CSRecord.libraryId = libraryIdGenerator.getLibraryId(readSAM)
//      CSRecord.index = index
//      CSRecord.readgroupid = readSAM.getAttribute(ReservedTagConstants.READ_GROUP_ID).toString
//
//      if(readSAM.getReadPairedFlag && !readSAM.getMateUnmappedFlag)
//        CSRecord.mateReferenceIndex = readSAM.getMateReferenceIndex
//
//      CSRecord
//    }

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

//  def buildFragSortRead(header : SAMFileHeader, rec : CSAlignmentRecord, libraryIdGenerator : LibraryIdGenerator) : CSAlignmentRecord = {
//    rec.read1ReferenceIndex = Integer2int(rec.getReferenceIndex)
//    if (rec.getReadNegativeStrandFlag) {
//      rec.read1Coordinate = rec.getUnclippedEnd
//      rec.orientation = ReadEnds.R
//    } else {
//      rec.read1Coordinate = rec.getUnclippedStart
//      rec.orientation = ReadEnds.F
//    }
//    rec.read1IndexInFile = rec.getIndex
//    if (rec.getReadPairedFlag && !rec.getMateUnmappedFlag) {
//      rec.read2ReferenceIndex = rec.getMateReferenceIndex.asInstanceOf[Int]
//      rec.paired = true
//    }
//
//    // Build one ReadEndsForMarkDuplicates for filling in optical duplicate location information
//    val ends: ReadEndsForMarkDuplicates = new ReadEndsForMarkDuplicates()
//    ends.read1ReferenceIndex = Integer2int(rec.getReferenceIndex)
//    if (rec.getReadNegativeStrandFlag) {
//      ends.read1Coordinate = rec.getUnclippedEnd
//      ends.orientation = ReadEnds.R
//    } else {
//      ends.read1Coordinate = rec.getUnclippedStart
//      ends.orientation = ReadEnds.F
//    }
//    ends.read1IndexInFile = rec.getIndex
//    ends.score = rec.getScore
//    if (rec.getReadPairedFlag && !rec.getMateUnmappedFlag)
//      ends.read2ReferenceIndex = rec.getMateReferenceIndex.asInstanceOf[Int]
//    ends.libraryId = rec.getLibraryId
//
//    if (this.opticalDuplicateFinder.addLocationInformation(rec.getReadName, ends)) {
//      // calculate the RG number (nth in list)
//      rec.tile = ends.getTile
//      rec.x = ends.x
//      rec.y = ends.y
//      rec.readGroup = 0
//      val rg: String = rec.getAttribute
//      val readGroups : List[SAMReadGroupRecord] = header.getReadGroups
//      val it = readGroups.iterator
//
//      if (rg != null && readGroups != null) {
//        while (it.hasNext) {
//          if (!it.next.getReadGroupId.equals(rg))
//            rec.readGroup = (rec.readGroup + 1).toShort
//        }
//      }
//    }
//
//    rec
//  }

//    def findMate (tmp : CSAlignmentQueuedMap[String, CSAlignmentRecord], key : String) : CSAlignmentRecord = {
//      // Find out the paired read that was already been processed but did not find their mate
//      // Return the read or return null if no finding
//      val mate = tmp.remove(key)
////      val it = tmp.iterator
////      while (it.hasNext) {
////        val tmpRecord : CSAlignmentRecord = it.next
////        if (tmpRecord.getReferenceIndex == referenceIndex && tmpRecord.getReadName == readName)
////          tmpRecord
////      }
//      /*for (target : CSAlignmentRecord <- tmp) {
//        if (target.getReferenceIndex == referenceIndex && target.getReadName == readName)
//          target
//      }*/
//      mate
//    }

    def generateDupIndexes(libraryIdGenerator : LibraryIdGenerator) = {
      val maxInMemory = Math.min(((Runtime.getRuntime.maxMemory() * 0.25) / SortingLongCollection.SIZEOF).toInt, (Integer.MAX_VALUE - 5).toDouble).toInt
      this.duplicateIndexes = new SortingLongCollection(maxInMemory, tempDir)
      // Generate the duplicate indexes for remove duplicate reads
      println("*** Start to generate duplicate indexes! ***\n")

      var firstOfNextChunk : ReadEndsForMarkDuplicates = null
      val nextChunk = new ArrayList[ReadEndsForMarkDuplicates](200)

      val itPairSort = this.pairSort.iterator
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
      this.pairSort.cleanup()
      this.pairSort = null

      var containsPairs : Boolean = false
      var containsFrags : Boolean = false

      val itFragSort = fragSort.iterator
      while (itFragSort.hasNext) {
        val next = itFragSort.next
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
      this.fragSort.cleanup()
      this.fragSort = null

      println("*** Finish generating duplicate indexes! ***\n")
      println("*** [REAL]The number of duplicate is: " + this.numDuplicateIndices + "\n")
      this.duplicateIndexes.doneAddingStartIteration()
    }

    def areComparableForDuplicates(lhs : ReadEndsForMarkDuplicates, rhs : ReadEndsForMarkDuplicates, compareRead2 : Boolean) : Boolean = {
      var retval: Boolean = (lhs.libraryId == rhs.libraryId) && (lhs.read1ReferenceIndex == rhs.read1ReferenceIndex) && (lhs.read1Coordinate == rhs.read1Coordinate) && (lhs.orientation == rhs.orientation)

      if (retval && compareRead2) {
        retval = (lhs.read2ReferenceIndex == rhs.read2ReferenceIndex) && (lhs.read2Coordinate == rhs.read2Coordinate)
      }

      retval
    }

//    def areComparableForDuplicatesF(lhs : CSAlignmentRecord, rhs : CSAlignmentRecord, compareRead2 : Boolean) : Boolean = {
//      var retval: Boolean = (lhs.libraryId == rhs.libraryId) && (lhs.read1ReferenceIndex == rhs.read1ReferenceIndex) && (lhs.read1Coordinate == rhs.read1Coordinate) && (lhs.orientation == rhs.orientation)
//
//      if (retval && compareRead2) {
//        retval = (lhs.read2ReferenceIndex == rhs.read2ReferenceIndex) && (lhs.read2Coordinate == rhs.read2Coordinate)
//      }
//
//      retval
//    }

    def addIndexAsDuplicate(bamIndex : Long) = {
      //println("we are adding No." + bamIndex + " duplicate index")
      this.duplicateIndexes.add(bamIndex)
      this.numDuplicateIndices += 1
    }

    def markDuplicatePairs(list : ArrayList[ReadEndsForMarkDuplicates], libraryIdGenerator : LibraryIdGenerator) = {
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
      //println("test max score:" + maxScore)

      val it2nd = list.iterator
      while (it2nd.hasNext) {
        val end = it2nd.next
        if (end != best) {
          //println("test read 1 & 2 index:" + end.read1IndexInFile + "; " + end.read2IndexInFile)
          addIndexAsDuplicate(end.read1IndexInFile)
          addIndexAsDuplicate(end.read2IndexInFile)
        }
      }

      // Null for libraryIdGenerator, need to figure out how to get SAM/BAM header
      if (OpticalDuplicateFinder.DEFAULT_READ_NAME_REGEX != null) {
        AbstractMarkDuplicatesCommandLineProgram.trackOpticalDuplicates(list, opticalDuplicateFinder, libraryIdGenerator)
      }
    }

    def markDuplicateFragments(list : ArrayList[ReadEndsForMarkDuplicates], containsPairs : Boolean) = {
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

//  def markDuplicateFragmentsF(list : java.util.ArrayList[CSAlignmentRecord], containsPairs : Boolean) = {
//    if (containsPairs) {
//      val it1st = list.iterator
//      while (it1st.hasNext) {
//        val end = it1st.next
//        if (!end.isPaired) addIndexAsDuplicate(end.read1IndexInFile)
//      }
//    } else {
//      var maxScore : Short = 0
//      var best : CSAlignmentRecord = null
//      val it2nd = list.iterator
//      while (it2nd.hasNext) {
//        val end = it2nd.next
//        if (end.score > maxScore || best == null) {
//          maxScore = end.score
//          best = end
//        }
//      }
//      val it3rd = list.iterator
//      while (it3rd.hasNext) {
//        val end = it3rd.next
//        if (end != best) {
//          addIndexAsDuplicate(end.read1IndexInFile)
//        }
//      }
//    }
//  }

    def writeToADAM(output : String, readsrdd : RDD[AlignmentRecord], sc : SparkContext) = {
      // Write results to ADAM file on HDFS
      println("*** Start to write reads back to ADAM file without duplicates! ***\n")

      // Transform duplicateIndexes into HashTable
      val dpIndexes = new mutable.HashSet[Long]()
      var dpCounter = 0
      while (this.duplicateIndexes.hasNext) {
        val value = this.duplicateIndexes.next
        dpIndexes += value
        dpCounter += 1
      }

      println("*** [COUNT]The number of duplicate is: " + dpCounter + " ; " + dpIndexes.size + " !***\n")
      /*for (index <- duplicateIndexes) {
        dpIndexes.put(index, index)
      }*/

      // Broadcast the duplicate indexes to nodes
      val broadcastDpIndexes = sc.broadcast(dpIndexes)

      val readADAMRDD = readsrdd.zipWithIndex().map{case (read : AlignmentRecord, index : Long) => {
        if (broadcastDpIndexes.value.contains(index)) {
          read.setDuplicateRead(boolean2Boolean(true))
          //println("!!!Got one duplicates!!!")
        } else read.setDuplicateRead(boolean2Boolean(false))

        read
      }}

      //val temp = readADAMRDD.collect()

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
      println("*** Finish marking duplicates based on duplicate index! ***\n")

      // Use the filter function to get rid of those reads contains indexes in the duplicateIndexes
      val saveADAMRDDFilter = readADAMRDD.filter(read  => read.getDuplicateRead.equals(false))

      println("*** Finish filter out duplicates! ***\n")

      saveADAMRDDFilter.adamParquetSave(output)


      //val saveADAMRDD = new ADAMRDDFunctions(saveADAMRDDFilter)
      //println("*** The number of reads after mark duplicate: " + saveADAMRDDFilter.count() + "\n")
      //saveADAMRDD.adamParquetSave(output)

      println("*** Finish writing back reads to ADAM file without duplicates! ***\n")
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

      val conf = new SparkConf().setAppName("Mark Duplicate").setMaster("spark://10.0.1.2:7077").set("spark.driver.maxResultSize", "100G").set("spark.network.timeout", "1000s").set("spark.network.timeout", "1000s").set("spark.cores.max", "500")
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
