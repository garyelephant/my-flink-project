package myflink.flink.formats.lucene;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base InputFormat class to read from Lucene files.
 * TODO: implement other FileInputFormat methods()
 */
public class LuceneRowInputFormat
  extends FileInputFormat<Row> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(LuceneRowInputFormat.class);

  private String[] fieldNames;

  private TypeInformation[] fieldTypes;

  private transient Counter recordConsumed;

  private transient Object expectedFileSchema;

  private transient LuceneRecordReader luceneRecordReader;


  public LuceneRowInputFormat(Path path, Object schema) {

    super(path);
    this.expectedFileSchema = checkNotNull(schema, "messageType");
    RowTypeInfo rowTypeInfo = (RowTypeInfo) LuceneSchemaConverter.fromLuceneType(expectedFileSchema);
    this.fieldTypes = rowTypeInfo.getFieldTypes();
    this.fieldNames = rowTypeInfo.getFieldNames();

    // read whole lucene file as one file split
    this.unsplittable = true;
  }

  @Override
  public void configure(Configuration parameters) {
    super.configure(parameters);
    // empty method
  }

  @Override
  public void open(FileInputSplit split) throws IOException {

    // TODO:
    //    get es index shard Lucene directory and files.
    //    recursively ...

    this.luceneRecordReader = new LuceneRecordReader();
    this.luceneRecordReader.initialize();

    if (this.recordConsumed == null) {
      this.recordConsumed = getRuntimeContext().getMetricGroup().counter("lucene-records-consumed");
    }
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return this.luceneRecordReader.reachEnd();
  }

  @Override
  public Row nextRecord(Row reuse) throws IOException {

    if (reachedEnd()) {
      return null;
    }

    recordConsumed.inc();

    return this.luceneRecordReader.nextRecord();
  }

  @Override
  public void close() throws IOException {
    if (this.luceneRecordReader != null) {
      this.luceneRecordReader.close();
    }
  }

  /**
   * Computes the input splits for the file. By default, one file block is one split. If more splits
   * are requested than blocks are available, then a split may be a fraction of a block and splits may cross
   * block boundaries.
   *
   * @param minNumSplits The minimum desired number of file splits.
   * @return The computed file splits.
   *
   * @see org.apache.flink.api.common.io.InputFormat#createInputSplits(int)
   */
  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {

    // TODO:
    return null;
  }

  /**
   * Configures the fields to be read and returned by the LuceneRowInputFormat. Selected fields must be present
   * in the configured schema.
   *
   * @param fieldNames Names of all selected fields.
   */
  public void selectFields(String[] fieldNames) {

    checkNotNull(fieldNames, "fieldNames");
    this.fieldNames = fieldNames;
    RowTypeInfo rowTypeInfo = (RowTypeInfo) LuceneSchemaConverter.fromLuceneType(expectedFileSchema);
    TypeInformation[] selectFieldTypes = new TypeInformation[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i++) {
      try {
        selectFieldTypes[i] = rowTypeInfo.getTypeAt(fieldNames[i]);
      } catch (IndexOutOfBoundsException e) {
        throw new IllegalArgumentException(String.format("Fail to access Field %s , "
          + "which is not contained in the file schema", fieldNames[i]), e);
      }
    }
    this.fieldTypes = selectFieldTypes;
  }
}
