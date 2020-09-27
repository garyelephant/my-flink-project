package myflink.flink.formats.lucene;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;


/**
 * A TableSource to read Lucene files exported from Elasticsearch.
 *
 * <p>The {@link LuceneTableSource} supports projection push-down.</p>
 *
 * <p>An {@link LuceneTableSource} is used as shown in the example below.
 *
 * <pre>
 * {@code
 * LuceneTableSource luceneSrc = LuceneTableSource.builder()
 *   .path("file:///...")
 *   .schema(...)
 *   .build();
 *
 * tEnv.registerTableSource("luceneTable", luceneSrc);
 * Table res = tableEnv.sqlQuery("SELECT * FROM luceneTable");
 * }
 * </pre>
 */
public class LuceneTableSource extends InputFormatTableSource<Row> implements ProjectableTableSource<Row> {

  private static final Logger LOG = LoggerFactory.getLogger(LuceneTableSource.class);

  // path to read Parquet files from
  private final String path;

  // TODO: JSON Java Class
  // schema of the Lucene file
  private final Object schema;

  // type information of the data returned by the InputFormat
  private final RowTypeInfo typeInfo;

  @Nullable
  private final int[] selectedFields;

  // the schema of table
  private final TableSchema tableSchema;

  private LuceneTableSource(String path, Object schema) {

    this(path, schema, null);
  }

  /**
   * @param path
   * @param schema elasticsearch index mappings (how to deal with index type ?)
   * @param selectedFields
   * */
  private LuceneTableSource(String path, Object schema, @Nullable int[] selectedFields) {

    this.path = path;
    this.schema = schema;
    this.selectedFields = selectedFields;

    // determine the type information from the Lucene schema
    RowTypeInfo typeInfoFromSchema = (RowTypeInfo) LuceneSchemaConverter.fromLuceneType(schema);

    if (selectedFields == null) {

      this.typeInfo = typeInfoFromSchema;
    } else {

      this.typeInfo = RowTypeInfo.projectFields(typeInfoFromSchema, selectedFields);
    }

    // create a TableSchema that corresponds to the Lucene schema
    this.tableSchema = TableSchema.builder()
      .fields(
        typeInfoFromSchema.getFieldNames(),
        (DataType[]) Arrays.stream(
          typeInfoFromSchema.getFieldTypes())
          .map(TypeConversions::fromLegacyInfoToDataType)
          .toArray())
      .build();
  }

  @Override
  public TableSource<Row> projectFields(int[] fields) {
    return new LuceneTableSource(path, schema, fields);
  }

  @Override
  public InputFormat<Row, ?> getInputFormat() {

    LuceneRowInputFormat luceneRowInputFormat =
      new LuceneRowInputFormat(new Path(this.path), this.schema);

    if (selectedFields != null) {
      luceneRowInputFormat.selectFields(typeInfo.getFieldNames());
    }

    return luceneRowInputFormat;
  }

  @Override
  public DataType getProducedDataType() {
    // TODO:
    return null;
  }

  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }

  @Override
  public String explainSource() {
    return String.format("[LuceneTableSource(path=%s)]", this.path);
  }

  // Builder
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Constructs an {@link LuceneTableSource}.
   */
  public static class Builder {

    private String path;

    private Object schema;

    /**
     * Sets the path of Lucene files.
     * If the path is specifies a directory, it will be recursively enumerated.
     *
     * @param path the path of the Parquet files.
     * @return The Builder
     */
    public Builder path(String path) {
      Preconditions.checkNotNull(path, "Path must not be null");
      Preconditions.checkArgument(!path.isEmpty(), "Path must not be empty");
      this.path = path;
      return this;
    }


    /**
     * Sets the Lucene schema of the files to read as a String.
     *
     * @param schema The parquet schema of the files to read as a String.
     * @return The Builder
     */
    public Builder schema(Object schema) {
      Preconditions.checkNotNull(schema, "Lucene schema must not be null");
      this.schema = schema;
      return this;
    }

    /**
     * Builds the LuceneTableSource for this builder.
     *
     * @return The LuceneTableSource for this builder.
     */
    public LuceneTableSource build() {
      Preconditions.checkNotNull(path, "Path must not be null");
      Preconditions.checkNotNull(schema, "Lucene schema must not be null");

      return new LuceneTableSource(this.path, this.schema);
    }

  }
}
