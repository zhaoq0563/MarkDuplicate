///////////////////////////////////////////////////////////////
//***                                                     ***//
//*** Mark Duplicate by using ADAM format                 ***//
//***                                                     ***//
//***	Created by Qi @ Oct. 2015                           ***//
//***                                                     ***//
//***	CS Department, UCLA                                 ***//
//***                                                     ***//
///////////////////////////////////////////////////////////////

package main.scala.csadam

import java.util.{Calendar, Comparator, ArrayList}
import java.io.{File, BufferedWriter, FileWriter}
import cs.ucla.edu.bwaspark.datatype.{BNTSeqType, BWAIdxType}
import cs.ucla.edu.bwaspark.sam.SAMHeader
import main.scala.csadam.util.CSAlignmentRecord
import htsjdk.samtools.util.Log
import htsjdk.samtools.DuplicateScoringStrategy.ScoringStrategy
import htsjdk.samtools._
import htsjdk.samtools.util.{SortingCollection, SortingLongCollection}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.SAMFileHeaderWritable
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord
import picard.sam.markduplicates.util._
import org.bdgenomics.adam.rdd.ADAMContext._

import scala.collection.mutable

object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {

    var maxInMemory : Int = (Runtime.getRuntime.maxMemory() * 0.2 / ReadEndsForMarkDuplicates.SIZE_OF).toInt
    val tempDir = new File(System.getProperty("java.io.tmpdir"))
    var pairSort : SortingCollection[ReadEndsForMarkDuplicates] = SortingCollection.newInstance[ReadEndsForMarkDuplicates](classOf[ReadEndsForMarkDuplicates], new ReadEndsForMarkDuplicatesCodec(), new ReadEndsComparator, maxInMemory, tempDir)
    var fragSort : SortingCollection[ReadEndsForMarkDuplicates] = SortingCollection.newInstance[ReadEndsForMarkDuplicates](classOf[ReadEndsForMarkDuplicates], new ReadEndsForMarkDuplicatesCodec(), new ReadEndsComparator, maxInMemory, tempDir)
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

      val readCSIndexRDD = readRDDwithZip.map{case (read : AlignmentRecord, index : Long) =>
        val samHeader = new SAMHeader
        val samFileHeader = new SAMFileHeader
        val packageVersion = "v01"
        val readGroupString = "@RG\tID:Sample_WGC033798D\tLB:Sample_WGC033798D\tSM:Sample_WGC033798D"
        samHeader.bwaGenSAMHeader(bns_bc.value, packageVersion, readGroupString, samFileHeader)
        val libraryIdGenerator = new LibraryIdGenerator(samFileHeader)

        val CSRecord : CSAlignmentRecord = buildCSAlignmentRecord(read, index, samFileHeader, libraryIdGenerator)

        CSRecord
      }
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

      var end = 0
      var count = 0
      var index = 0
      var totalTake = 0
      val partSize = 150000000
      //val iteration = totalReads/partSize + 1
      val tmp = new DiskBasedReadEndsForMarkDuplicatesMap(MAX_NUMBER_FOR_READ_MAP)

      println("*** Start to build fragSort and pairSort! ***\n")

      while(end == 0) {
        // Collect 10 million each iteration to build fragSort and PairSort
        // 1, Filter out those already been built and convert reads to CSAlignmentRecord
        var readIterationRDD = readCSIndexRDD.filter{read : CSAlignmentRecord => {read.getIndex >= (count * partSize) && read.getIndex <= ((count+1) * partSize - 1)}}

        // 2, Collect back the first partSize reads
        var readArray = readIterationRDD.collect()
        println("*** " + readArray.length + " reads are taken for this iteration! ***\n")
        val gb = 1024*1024*1024
        val runtime = Runtime.getRuntime
        println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / gb)
        println("** Free Memory:  " + runtime.freeMemory / gb)
        println("** Total Memory: " + runtime.totalMemory / gb)
        println("** Max Memory:   " + runtime.maxMemory / gb)
        totalTake = readArray.length
        if (totalTake != partSize) {
          end = 1
        }

        println("*** Process on " + (count * partSize) + " to "  + (count * partSize + totalTake) + " reads! ***\n")

        // 3, Build the fragSort and PairSort
        val it = readArray.iterator
        var continue = 1
        while(it.hasNext && continue == 1) {
          val readCSRecord = it.next()

          if (readCSRecord.getReadUnmappedFlag) {
            if (readCSRecord.getReferenceIndex == -1) {
              continue = 0
              index -= 1
            }
          }
          else if (!readCSRecord.isSecondaryOrSupplementary) {
            val fragmentEnd = buildReadEnds(header, readCSRecord)
            this.fragSort.add(fragmentEnd)

            if (readCSRecord.getReadPairedFlag && !readCSRecord.getMateUnmappedFlag) {
              val key = readCSRecord.getReadGroupID + ":" + readCSRecord.getReadName
              var checkPair = tmp.remove(Integer2int(readCSRecord.getReferenceIndex), key)
              if (checkPair == null) {
                checkPair = buildReadEnds(header, readCSRecord)
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
        readArray = null
        count += 1
        readIterationRDD = null
        println("** [After]Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / gb)
        println("** [After]Free Memory:  " + runtime.freeMemory / gb)
        println("** [After]Total Memory: " + runtime.totalMemory / gb)
        println("** [After]Max Memory:   " + runtime.maxMemory / gb)
        System.gc()
      }

      println("*** Read " + index + " records. " + tmp.size() + " pairs never matched. ***\n")

      fragSort.doneAdding()
      pairSort.doneAdding()

      println("*** Finish building pairSort and fragSort! ***\n")

      // Write two sorts to files
      val pw = new BufferedWriter(new FileWriter(new File("/curr/qzhao/logs/intermediate/csmarkduplicate.txt")))

      val it = pairSort.iterator()
      while (it.hasNext) {
        val pair = it.next()
        pw.append("Read1RefIndex: " + pair.read1ReferenceIndex.toString + "\tRead1Coordinate: " + pair.read1Coordinate.toString + "\tRead1IndexinFile: " + pair.read1IndexInFile.toString + "\tRead2RefIndex: " + pair.read2ReferenceIndex.toString + "\tRead2Coordinate: " + pair.read2Coordinate.toString + "\tRead2IndexinFile: " + pair.read2IndexInFile.toString + "\tOrientation: " + pair.orientation.toString + "\tOrientationForOpt: " + pair.orientationForOpticalDuplicates.toString + "\tScore: " + pair.score.toString + "\tLibraryID: " + pair.libraryId.toString + "\tReadGroup: " + pair.getReadGroup.toString).write("\n")
      }
      pw.flush
      pw.close
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
      builder.setReadName(adamRecord.getReadName)
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
        .foreach(v => builder.setAttribute("LB", v))
      Option(adamRecord.getRecordGroupPlatformUnit)
        .foreach(v => builder.setAttribute("PU", v))

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

      // return sam record
      builder
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

    def buildReadEnds(header : SAMFileHeader, rec : CSAlignmentRecord) : ReadEndsForMarkDuplicates = {
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
        val readGroups = header.getReadGroups
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

    def generateDupIndexes(libraryIdGenerator : LibraryIdGenerator) = {
      val maxInMemory = Math.min(((Runtime.getRuntime.maxMemory() * 0.2) / SortingLongCollection.SIZEOF).toInt, (Integer.MAX_VALUE - 5).toDouble).toInt
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

    def addIndexAsDuplicate(bamIndex : Long) = {
      //println("we are adding No." + bamIndex + " duplicate index")
      this.duplicateIndexes.add(bamIndex)
      this.numDuplicateIndices += 1
    }

    def markDuplicatePairs(list : ArrayList[ReadEndsForMarkDuplicates], libraryIdGenerator : LibraryIdGenerator) = {
      var maxScore : Short = 0
      var best : ReadEndsForMarkDuplicates = null

      if (list.size > 100000) println(Calendar.getInstance.getTime() + " [Pair chunk size: " + list.size() + "]")

      /** All read ends should have orientation FF, FR, RF, or RR **/
      val it1st = list.iterator
      while (it1st.hasNext) {
        val end = it1st.next
        if (end.score > maxScore || best == null) {
          maxScore = end.score
          best = end
        }
      }

      val it2nd = list.iterator
      while (it2nd.hasNext) {
        val end = it2nd.next
        if (end != best) {
          //println("test read 1 & 2 index:" + end.read1IndexInFile + "; " + end.read2IndexInFile)
          addIndexAsDuplicate(end.read1IndexInFile)
          addIndexAsDuplicate(end.read2IndexInFile)
        }
      }

      if (this.READ_NAME_REGEX != null) {
        AbstractMarkDuplicatesCommandLineProgram.trackOpticalDuplicates(list, opticalDuplicateFinder, libraryIdGenerator)
      }
    }

    def markDuplicateFragments(list : ArrayList[ReadEndsForMarkDuplicates], containsPairs : Boolean) = {

      if (list.size > 100000) println(Calendar.getInstance.getTime() + " [Frag chunk size: " + list.size() + "]")

      if (containsPairs) {
        val it1st = list.iterator
        while (it1st.hasNext) {
          val end = it1st.next
          if (!end.isPaired) addIndexAsDuplicate(end.read1IndexInFile)
        }
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
      val dpIndexes = new mutable.HashSet[Long]()
      var dpCounter = 0
      while (this.duplicateIndexes.hasNext) {
        val value = this.duplicateIndexes.next
        dpIndexes += value
        dpCounter += 1
      }

      println("*** [COUNT]The number of duplicate is: " + dpCounter + " ; " + dpIndexes.size + " !***\n")

      // Broadcast the duplicate indexes to nodes
      val broadcastDpIndexes = sc.broadcast(dpIndexes)

      val readADAMRDD = readsrdd.zipWithIndex().map{case (read : AlignmentRecord, index : Long) =>
        if (broadcastDpIndexes.value.contains(index)) {
          read.setDuplicateRead(boolean2Boolean(true))
        } else read.setDuplicateRead(boolean2Boolean(false))

        read
      }

      println("*** Finish marking duplicates based on duplicate index! ***\n")

      // Use the filter function to get rid of those reads contains indexes in the duplicateIndexes
      val saveADAMRDDFilter = readADAMRDD.filter(read  => read.getDuplicateRead.equals(false))

      println("*** Finish filter out duplicates! ***\n")

      saveADAMRDDFilter.adamParquetSave(output)

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

      val conf = new SparkConf().setAppName("CS-MarkDuplicate").setMaster("spark://10.0.1.2:7077").set("spark.driver.maxResultSize", "100G").set("spark.network.timeout", "5000s").set("spark.cores.max", "500").set("spark.cleaner.ttl", "43200")
      val sc = new SparkContext(conf)
      val ac = new ADAMContext(sc)
      val readsRDD: RDD[AlignmentRecord] = ac.loadAlignments(input)

      println("*** The alignments are loaded successfully! ***\n")

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

      buildSortList(input, readsRDD, bns_bc, samFileHeader, libraryIdGenerator, sc)
      // @ Need to be done
      /*generateDupIndexes(libraryIdGenerator) // Passing bwaIdx_bc and access it on workers instead of creating a new one here
      writeToADAM(output, readsRDD, sc)

      // @ Need to be done
      val numOpticalDuplicates = libraryIdGenerator.getOpticalDuplicatesByLibraryIdMap.getSumOfValues.toLong
      println("*** The number of optical duplicates are : " + numOpticalDuplicates + " !***\n")

      val t1 = System.nanoTime() : Double

      println("*** Mark duplicate has been successfully done! ***\n")
      println("*** The total time for Mark Duplicate is : " + (t1-t0) / 1000000000.0 + " secs.\n")*/
    }
}
