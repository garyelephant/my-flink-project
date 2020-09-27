package myflink.flink.formats.lucene;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;
import java.io.IOException;

public class LuceneRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(LuceneRecordReader.class);

  private boolean readRecordReturned = true;
  private Row readRecord;

  // number of records in file
  private long numTotalRecords;
  // number of records that were read from file
  private long numReadRecords = 0;

  private Object luceneIndexReader;


  public LuceneRecordReader() {

    // TODO: params
  }

  public void initialize() {
    // TODO: initialize lucene index reader
  }

  /**
   * Checks if the record reader returned all records.
   * This method must be called before a record can be returned.
   *
   * @return False if there are more records to be read. True if all records have been returned.
   */
  public boolean reachEnd() throws IOException {
    // check if we have a read row that was not returned yet
    if (readRecord != null && !readRecordReturned) {
      return false;
    }
    // check if there are more rows to be read
    if (numReadRecords >= numTotalRecords) {
      return true;
    }
    // try to read next row
    return !readNextRecord();
  }

  /**
   * Returns the next record.
   * Note that the reachedEnd() method must be called before.
   *
   * @return The next record.
   */
  @CheckReturnValue(when = When.NEVER)
  public Row nextRecord() {
    readRecordReturned = true;
    return readRecord;
  }

  /**
   * Reads the next record.
   *
   * @return True if a record could be read, false otherwise.
   */
  private boolean readNextRecord() throws IOException {

    // TODO

    return true;
  }

  public void close() throws  IOException {
    if (luceneIndexReader != null) {
      // TODO:
      //    close luceneIndexReader
    }
  }

}
