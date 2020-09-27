package myflink.flink.formats.lucene;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

/**
 * Schema converter converts Parquet schema to and from Flink internal types.
 */
public class LuceneSchemaConverter {

  /**
   * Converts Parquet schema to Flink Internal Type.
   *
   * @param schema Lucene schema
   * @return Flink type information
   */
  public static TypeInformation<?> fromLuceneType(Object schema) {
    // TODO:
    return new RowTypeInfo();
  }
}
