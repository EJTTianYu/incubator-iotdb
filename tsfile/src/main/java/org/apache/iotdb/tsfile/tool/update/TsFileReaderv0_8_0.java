package org.apache.iotdb.tsfile.tool.update;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.TsDigest.StatisticType;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.reader.DefaultTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileReaderv0_8_0 implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileReaderv0_8_0.class);

  private TsFileInput tsFileInput;
  private long fileMetadataPos;
  private int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  protected String file;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.length() bytes
   * of the file for preparing reading real data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileReaderv0_8_0(String file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -load meta data size
   */
  public TsFileReaderv0_8_0(String file, boolean loadMetadataSize) throws IOException {
    this.file = file;
    final Path path = Paths.get(file);
    tsFileInput = new DefaultTsFileInput(path);
    try {
      if (loadMetadataSize) {
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileReaderv0_8_0(TsFileInput input, boolean loadMetadataSize)
      throws IOException {
    this.tsFileInput = input;
    try {
      if (loadMetadataSize) { // NOTE no autoRepair here
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  public void loadMetadataSize() throws IOException {
    ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
    tsFileInput.read(metadataSize,
        tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES);
    metadataSize.flip();
    // read file metadata size and position
    fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
    fileMetadataPos =
        tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES - fileMetadataSize;
    // skip the magic header
    tsFileInput.position(TSFileConfig.MAGIC_STRING.length());
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public String readTailMagic() throws IOException {
    long totalSize = tsFileInput.size();

    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.length());
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * whether the file is a complete TsFile: only if the head magic and tail magic string exists.
   */
  public boolean isComplete() throws IOException {
    return tsFileInput.size() >= TSFileConfig.MAGIC_STRING.length() * 2 && readTailMagic()
        .equals(readHeadMagic());
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public String readHeadMagic() throws IOException {
    return readHeadMagic(false);
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @param movePosition whether move the position of the file reader after reading the magic header
   * to the end of the magic head string.
   */
  public String readHeadMagic(boolean movePosition) throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
    if (movePosition) {
      tsFileInput.position(0);
      tsFileInput.read(magicStringBytes);
    } else {
      tsFileInput.read(magicStringBytes, 0);
    }
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public TsFileMetaData readFileMetadata() throws IOException {
    ByteBuffer buffer = readData(fileMetadataPos, fileMetadataSize);
    TsFileMetaData fileMetaData = new TsFileMetaData();

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      Map<String, TsDeviceMetadataIndex> deviceMap = new HashMap<>();
      String key;
      TsDeviceMetadataIndex value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = TsDeviceMetadataIndex.deserializeFrom(buffer);
        deviceMap.put(key, value);
      }
      fileMetaData.setDeviceIndexMap(deviceMap);
    }

    size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      fileMetaData.setMeasurementSchema(new HashMap<>());
      String key;
      MeasurementSchema value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = MeasurementSchema.deserializeFrom(buffer);
        fileMetaData.getMeasurementSchema().put(key, value);
      }
    }

    fileMetaData.setCurrentVersion(ReadWriteIOUtils.readInt(buffer));

    if (ReadWriteIOUtils.readIsNull(buffer)) {
      fileMetaData.setCreatedBy(ReadWriteIOUtils.readString(buffer));
    }

    return fileMetaData;
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public TsDeviceMetadata readTsDeviceMetaData(TsDeviceMetadataIndex index) throws IOException {
    return TsDeviceMetadata.deserializeFrom(readData(index.getOffset(), index.getLen()));
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupFooter readChunkGroupFooter() throws IOException {
    return ChunkGroupFooter.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br> This
   * method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader() throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type);
  }


  public long position() throws IOException {
    return tsFileInput.position();
  }

  public void position(long offset) throws IOException {
    tsFileInput.position(offset);
  }

  /**
   * read one byte from the input. <br> this method is not thread safe
   */
  public byte readMarker() throws IOException {
    markerBuffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, markerBuffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    markerBuffer.flip();
    return markerBuffer.get();
  }

  public void close() throws IOException {
    this.tsFileInput.close();
  }

  public String getFileName() {
    return this.file;
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position. <br> if position = -1, the tsFileInput's position will be changed to the current
   * position + real data size that been read. Other wise, the tsFileInput's position is not
   * changed.
   *
   * @param position the start position of data in the tsFileInput, or the current position if
   * position = -1
   * @param size the size of data that want to read
   * @return data that been read.
   */
  private ByteBuffer readData(long position, int size) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    if (position == -1) {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) != size) {
        throw new IOException("reach the end of the data");
      }
    } else {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer, position, size) != size) {
        throw new IOException("reach the end of the data");
      }
    }
    buffer.flip();
    return buffer;
  }

  /**
   * notice, the target bytebuffer are not flipped.
   */
  public int readRaw(long position, int length, ByteBuffer target) throws IOException {
    return ReadWriteIOUtils
        .readAsPossible(tsFileInput, target, position, length);
  }

  /**
   * Self Check the file and return the position before where the data is safe.
   */
  public boolean updateFile(String updateFileName) throws IOException {
    File checkFile = new File(this.file);
    File updateFile = new File(updateFileName);
    if (!updateFile.getParentFile().exists()) {
      updateFile.getParentFile().mkdirs();
    }
    updateFile.createNewFile();
    TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(updateFile);
    long fileSize;
    if (!checkFile.exists()) {
      logger.error("the file to be updated does not exist, file path: {}", checkFile.getPath());
      return false;
    } else {
      fileSize = checkFile.length();
    }
    //缓存
    List<ChunkHeader> chunkHeaders = new ArrayList<>();
    List<List<PageHeader>> pageHeadersList = new ArrayList<>();
    List<List<ByteBuffer>> pagesList = new ArrayList<>();
    Schema schema = null;

    String magic = readHeadMagic(true);
    if (!magic.equals("TsFilev0.8.0")) {
      logger.error("the file's MAGIC STRING is correct, file path: {}", checkFile.getPath());
      return false;
    }

    if (fileSize == TSFileConfig.MAGIC_STRING.length()) {
      logger.error("the file only contains magic string, file path: {}", checkFile.getPath());
      return false;
    } else if (readTailMagic().equals(magic)) {
      loadMetadataSize();
      TsFileMetaData tsFileMetaData = readFileMetadata();
      schema = new Schema(tsFileMetaData.getMeasurementSchema());
    }

    long startTimeOfChunk = 0;
    long endTimeOfChunk = 0;
    long numOfPoints = 0;
    ChunkMetaData currentChunkMetaData;
    List<ChunkMetaData> chunks = null;
    long startOffsetOfChunkGroup = 0;
    boolean newChunkGroup = true;
    long versionOfChunkGroup = 0;
    List<ChunkGroupMetaData> newMetaData = new ArrayList<>();
    List<Statistics<?>> chunkStatisticsList = new ArrayList<>();

    boolean goon = true;
    byte marker;
    try {
      while (goon && (marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            // this is the first chunk of a new ChunkGroup.
            if (newChunkGroup) {
              newChunkGroup = false;
              chunks = new ArrayList<>();
              startOffsetOfChunkGroup = this.position() - 1;
            }

            long fileOffsetOfChunk = this.position() - 1;
            ChunkHeader header = this.readChunkHeader();
            chunkHeaders.add(header);
            List<PageHeader> pageHeaders = new ArrayList<>();
            List<ByteBuffer> pages = new ArrayList<>();
            TSDataType dataType = header.getDataType();
            Statistics<?> chunkStatistics = Statistics.getStatsByType(dataType);
            chunkStatisticsList.add(chunkStatistics);
            if (header.getNumOfPages() > 0) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              pageHeaders.add(pageHeader);
              numOfPoints += pageHeader.getNumOfValues();
              startTimeOfChunk = pageHeader.getMinTimestamp();
              endTimeOfChunk = pageHeader.getMaxTimestamp();
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              pages.add(readData(-1, pageHeader.getCompressedSize()));
            }
            for (int j = 1; j < header.getNumOfPages() - 1; j++) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              pageHeaders.add(pageHeader);
              numOfPoints += pageHeader.getNumOfValues();
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              pages.add(readData(-1, pageHeader.getCompressedSize()));
            }
            if (header.getNumOfPages() > 1) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              pageHeaders.add(pageHeader);
              numOfPoints += pageHeader.getNumOfValues();
              endTimeOfChunk = pageHeader.getMaxTimestamp();
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              pages.add(readData(-1, pageHeader.getCompressedSize()));
            }

            currentChunkMetaData = new ChunkMetaData(header.getMeasurementID(), dataType,
                fileOffsetOfChunk,
                startTimeOfChunk, endTimeOfChunk);
            currentChunkMetaData.setNumOfPoints(numOfPoints);
            ByteBuffer[] statisticsArray = new ByteBuffer[StatisticType.getTotalTypeNum()];
            statisticsArray[StatisticType.min_value.ordinal()] = ByteBuffer
                .wrap(chunkStatistics.getMinBytes());
            statisticsArray[StatisticType.max_value.ordinal()] = ByteBuffer
                .wrap(chunkStatistics.getMaxBytes());
            statisticsArray[StatisticType.first_value.ordinal()] = ByteBuffer
                .wrap(chunkStatistics.getFirstBytes());
            statisticsArray[StatisticType.last_value.ordinal()] = ByteBuffer
                .wrap(chunkStatistics.getLastBytes());
            statisticsArray[StatisticType.sum_value.ordinal()] = ByteBuffer
                .wrap(chunkStatistics.getSumBytes());
            TsDigest tsDigest = new TsDigest();
            tsDigest.setStatistics(statisticsArray);
            currentChunkMetaData.setDigest(tsDigest);
            chunks.add(currentChunkMetaData);
            numOfPoints = 0;
            pageHeadersList.add(pageHeaders);
            pagesList.add(pages);
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            //this is a chunk group
            //if there is something wrong with the ChunkGroup Footer, we will drop this ChunkGroup
            //because we can not guarantee the correctness of the deviceId.
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            String deviceID = chunkGroupFooter.getDeviceID();
            long endOffsetOfChunkGroup = this.position();
            ChunkGroupMetaData currentChunkGroup = new ChunkGroupMetaData(deviceID, chunks,
                startOffsetOfChunkGroup);
            currentChunkGroup.setEndOffsetOfChunkGroup(endOffsetOfChunkGroup);
            currentChunkGroup.setVersion(versionOfChunkGroup++);
            newMetaData.add(currentChunkGroup);
            tsFileIOWriter.startChunkGroup(deviceID);
            for (int i = 0; i < chunkHeaders.size(); i++) {
              ChunkHeader chunkHeader = chunkHeaders.get(i);
              ChunkMetaData chunkMetaData = chunks.get(i);
              tsFileIOWriter
                  .startFlushChunk(schema.getMeasurementSchema(chunkHeader.getMeasurementID()),
                      chunkHeader.getCompressionType(), chunkHeader.getDataType(),
                      chunkHeader.getEncodingType(),
                      chunkStatisticsList.get(i), chunkMetaData.getEndTime(),
                      chunkMetaData.getStartTime(), chunkHeader.getDataSize(),
                      chunkHeader.getNumOfPages());
              List<PageHeader> pageHeaderList = pageHeadersList.get(i);
              List<ByteBuffer> pageList = pagesList.get(i);

              ChunkBuffer chunkBuffer = new ChunkBuffer(
                  schema.getMeasurementSchema(chunkHeader.getMeasurementID()));
              for (int j = 0; j < pageHeaderList.size(); j++) {
                chunkBuffer.writePageHeaderAndDataIntoBuff(pageList.get(j), pageHeaderList.get(j));
              }
              chunkBuffer.writeAllPagesOfSeriesToTsFile(tsFileIOWriter, chunkStatisticsList.get(i));
            }
            tsFileIOWriter.endChunkGroup(currentChunkGroup.getVersion());
            chunkStatisticsList.clear();
            chunkHeaders.clear();
            pageHeadersList.clear();
            pagesList.clear();
            newChunkGroup = true;
            break;

          default:
            // the disk file is corrupted, using this file may be dangerous
            logger.error("Unrecognized marker detected, this file may be corrupted");
            return false;
        }
      }
      tsFileIOWriter.endFile(schema);
      return true;
      // now we read the tail of the data section, so we are sure that the last ChunkGroupFooter is
      // complete.

    } catch (IOException | PageException e2) {
      logger.info("TsFile self-check cannot proceed at position {} after {} chunk groups "
          + "recovered, because : {}", this.position(), newMetaData.size(), e2.getMessage());
      return false;
    }
  }
}

