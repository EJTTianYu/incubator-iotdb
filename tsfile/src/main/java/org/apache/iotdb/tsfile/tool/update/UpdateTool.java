package org.apache.iotdb.tsfile.tool.update;

import static org.apache.iotdb.tsfile.write.writer.TsFileIOWriter.magicStringBytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateTool {

  private static final Logger logger = LoggerFactory.getLogger(UpdateTool.class);

  /**
   * 升级文件夹下所有的tsfile
   *
   * @param dir 文件夹路径
   * @param updateDir 修改后的文件夹
   */
  private static void updateTsfiles(String dir, String updateDir) throws IOException {
    //遍历查找所有的tsfile文件
    File file = new File(dir);
    List<File> tmp = new ArrayList<>();
    tmp.add(file);
    List<String> tsfiles = new ArrayList<>();
    if (file.exists()) {
      while (!tmp.isEmpty()) {
        File tmp_file = tmp.remove(0);
        File[] files = tmp_file.listFiles();
        for (File file2 : files) {
          if (file2.isDirectory()) {
            tmp.add(file2);
          } else {
            if (file2.getName().endsWith(".tsfile")) {
              tsfiles.add(file2.getAbsolutePath());
            }
          }
        }
      }
    }
    //对于每个tsfile文件，进行升级操作
    for (String tsfile : tsfiles) {
      updateOneTsfile(tsfile, tsfile.replace(dir, updateDir));
    }
  }

  /**
   * 升级单个tsfile文件
   *
   * @param tsfileName tsfile的绝对路径
   */
  private static void updateOneTsfile(String tsfileName, String updateFileName) throws IOException {
    TsFileReaderv0_8_0 updater = new TsFileReaderv0_8_0(tsfileName);
    updater.updateFile(updateFileName);
  }

  public static void main(String[] args) throws IOException {
    List<String> tsfileDirs = new ArrayList<>();
    List<String> tsfileDirsUpdate = new ArrayList<>();
    tsfileDirs.add("/Users/tianyu/2019秋季学期/incubator-iotdb/data/data");
    tsfileDirsUpdate.add("/Users/tianyu/2019秋季学期/incubator-iotdb/data/data1");
    for (int i = 0; i < tsfileDirs.size(); i++) {
      updateTsfiles(tsfileDirs.get(i), tsfileDirsUpdate.get(i));
    }
  }
}
