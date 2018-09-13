package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.SAMRecord;
import java.util.Iterator;

public class DebuggingSAMRecordIterator implements Iterator<SAMRecord> {

  private final Iterator<SAMRecord> it;
  private String source;
  private String readName;

  public DebuggingSAMRecordIterator(Iterator<SAMRecord> it, String source, String readName) {
    this.it = it;
    this.source = source;
    this.readName = readName;
  }

  @Override
  public boolean hasNext() {
    return it.hasNext();
  }

  @Override
  public SAMRecord next() {
    SAMRecord next = it.next();
    if (next.getReadName().equals(readName)) {
      System.out.println(
          "tw: found read name in "
              + source
              + " with file pointer "
              + next.getFileSource().getFilePointer());
    }
    return next;
  }
}
