package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Reader.Options;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 * @author juntao zhang
 */
public class OrcIndexInputFormat extends FileInputFormat<NullWritable, Text> {

  private final static StructType DATA_SCHEMA = new StructType(new StructField[]{
      new StructField("device", DataTypes.IntegerType, false, Metadata.empty())
  });

  @Override

  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    FileSplit fileSplit = (FileSplit) split;
    return new OrcRecordReader(fileSplit, context.getConfiguration());
  }

  private static class OrcRecordReader extends RecordReader<NullWritable, Text> {

    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    OrcStruct value;
    private float progress = 0.0f;
    SettableStructObjectInspector structOI;
    private String fileName;
    private Long rowNumber;

    OrcRecordReader(FileSplit fileSplit, Configuration conf)
        throws IOException {
      System.err.println(fileSplit.getPath());
      Reader file = OrcFile.createReader(fileSplit.getPath(), OrcFile.readerOptions(conf));
      Options options = (new Options()).range(fileSplit.getStart(), fileSplit.getLength());
      this.reader = file.rowsOptions(options);
//      this.reader = OrcInputFormat.createReaderFromFile(file, conf, fileSplit.getStart(), fileSplit.getLength());
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(DATA_SCHEMA.catalogString());
      this.structOI = (SettableStructObjectInspector) OrcStruct
          .createObjectInspector(typeInfo);
      this.value = (OrcStruct) structOI.create();
      this.fileName = fileSplit.getPath().getName();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      if (reader.hasNext()) {
        rowNumber = reader.getRowNumber();
        reader.next(value);
        progress = reader.getProgress();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() throws IOException {
      Text device = (Text) structOI.getStructFieldData(value, structOI.getStructFieldRef("device"));
      return new Text(
          device.toString() + "\u0001" + rowNumber + "\u0001" + this.fileName
      );
    }

    @Override
    public float getProgress() {
      return progress;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
