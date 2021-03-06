package org.disq_bio.disq;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class HtsjdkReadsRddTest extends BaseTest {

  private Object[] parametersForTestReadAndWrite() {
    return new Object[][] {
      {"1.bam", null, ReadsFormatWriteOption.BAM, 128 * 1024, false},
      {"1.bam", null, ReadsFormatWriteOption.BAM, 128 * 1024, true},
      {"valid.cram", "valid.fasta", ReadsFormatWriteOption.CRAM, 128 * 1024, false},
      {"valid.cram", "valid.fasta", ReadsFormatWriteOption.CRAM, 128 * 1024, true},
      {"valid_no_index.cram", "valid.fasta", ReadsFormatWriteOption.CRAM, 128 * 1024, false},
      {"test.sam", null, ReadsFormatWriteOption.SAM, 128 * 1024, false},
      {
        "gs://genomics-public-data/NA12878.chr20.sample.bam",
        null,
        ReadsFormatWriteOption.BAM,
        128 * 1024,
        true
      }
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(
      String inputFile,
      String cramReferenceFile,
      ReadsFormatWriteOption formatWriteOption,
      int splitSize,
      boolean useNio)
      throws Exception {
    String inputPath = getPath(inputFile);
    String refPath = getPath(cramReferenceFile);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(splitSize)
            .useNio(useNio)
            .referenceSourcePath(refPath);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath, refPath);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());

    // write the RDD back to a file
    String outputPath =
        createTempPath(SamFormat.fromFormatWriteOption(formatWriteOption).getExtension());
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath);

    // check the new file has the number of expected reads
    Assert.assertEquals(expectedCount, AnySamTestUtil.countReads(outputPath, refPath));
    if (SamtoolsTestUtil.isSamtoolsAvailable()) {
      Assert.assertEquals(expectedCount, SamtoolsTestUtil.countReads(outputPath, refPath));
    }

    // check we can read back what we've just written
    Assert.assertEquals(expectedCount, htsjdkReadsRddStorage.read(outputPath).getReads().count());
  }

  private Object[] parametersForTestReadUsingSBIIndex() {
    return new Object[][] {
      {"1-with-splitting-index.bam", 128 * 1024, false},
      {"1-with-splitting-index.bam", 128 * 1024, true},
    };
  }

  @Test
  @Parameters
  public void testReadUsingSBIIndex(String inputFile, int splitSize, boolean useNio)
      throws Exception {
    String inputPath = getPath(inputFile);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(splitSize).useNio(useNio);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());
  }

  private Object[] parametersForTestReadAndWriteMultiple() {
    return new Object[][] {
      {null, false, ReadsFormatWriteOption.BAM},
      {"test.fa", false, ReadsFormatWriteOption.CRAM},
      {null, false, ReadsFormatWriteOption.SAM},
    };
  }

  @Test
  @Parameters
  public void testReadAndWriteMultiple(
      String cramReferenceFile, boolean useNio, ReadsFormatWriteOption formatWriteOption)
      throws Exception {

    String refPath = getPath(cramReferenceFile);

    // Read in a single large (generated) BAM/CRAM/SAM file
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.coordinate, formatWriteOption, refPath);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(40000)
            .useNio(useNio)
            .referenceSourcePath(refPath);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    // check that there are multiple partitions
    Assert.assertTrue(htsjdkReadsRdd.getReads().getNumPartitions() > 1);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath, refPath);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());

    // write as multiple BAM/CRAM/SAM files
    String outputPath = createTempPath("");
    htsjdkReadsRddStorage.write(
        htsjdkReadsRdd, outputPath, FileCardinalityWriteOption.MULTIPLE, formatWriteOption);

    // check the new file has the number of expected reads
    int totalCount = 0;
    for (String part : listPartFiles(outputPath)) {
      totalCount += AnySamTestUtil.countReads(part, refPath);
    }
    Assert.assertEquals(expectedCount, totalCount);

    if (SamtoolsTestUtil.isSamtoolsAvailable()) {
      int totalCountSamtools = 0;
      for (String part : listPartFiles(outputPath)) {
        totalCountSamtools += SamtoolsTestUtil.countReads(part, refPath);
      }
      Assert.assertEquals(expectedCount, totalCountSamtools);
    }

    // check we can read back what we've just written
    Assert.assertEquals(expectedCount, htsjdkReadsRddStorage.read(outputPath).getReads().count());
  }

  private Object[] parametersForTestReadIntervals() {
    return new Object[][] {
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ReadsFormatWriteOption.BAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ReadsFormatWriteOption.BAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            true),
        ReadsFormatWriteOption.BAM
      },
      {null, new HtsjdkReadsTraversalParameters<>(null, true), ReadsFormatWriteOption.BAM},
      {
        null,
        new HtsjdkReadsTraversalParameters<>(Collections.emptyList(), true),
        ReadsFormatWriteOption.BAM
      },
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ReadsFormatWriteOption.CRAM
      },
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ReadsFormatWriteOption.CRAM
      },
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            true),
        ReadsFormatWriteOption.CRAM
      },
      {"test.fa", new HtsjdkReadsTraversalParameters<>(null, true), ReadsFormatWriteOption.CRAM},
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(Collections.emptyList(), true),
        ReadsFormatWriteOption.CRAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ReadsFormatWriteOption.SAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ReadsFormatWriteOption.SAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            true),
        ReadsFormatWriteOption.SAM
      },
      {null, new HtsjdkReadsTraversalParameters<>(null, true), ReadsFormatWriteOption.SAM},
      {
        null,
        new HtsjdkReadsTraversalParameters<>(Collections.emptyList(), true),
        ReadsFormatWriteOption.SAM
      },
    };
  }

  @Test
  @Parameters
  public <T extends Locatable> void testReadIntervals(
      String cramReferenceFile,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ReadsFormatWriteOption formatWriteOption)
      throws Exception {
    String refPath = getPath(cramReferenceFile);

    // Read in a single large (generated) BAM/CRAM/SAM file
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.coordinate, formatWriteOption, refPath);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(40000)
            .useNio(false)
            .referenceSourcePath(refPath);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath, traversalParameters);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath, refPath, traversalParameters);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());

    // also check the count with samtools (except for SAM since it cannot do intervals)
    if (SamtoolsTestUtil.isSamtoolsAvailable()
        && !formatWriteOption.equals(ReadsFormatWriteOption.SAM)) {
      int expectedCountSamtools =
          SamtoolsTestUtil.countReads(inputPath, refPath, traversalParameters);
      Assert.assertEquals(expectedCountSamtools, htsjdkReadsRdd.getReads().count());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMappedOnlyFails() throws Exception {
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.coordinate, ReadsFormatWriteOption.BAM, null);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(40000).useNio(false);

    htsjdkReadsRddStorage.read(inputPath, new HtsjdkReadsTraversalParameters<>(null, false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWritingToADirectoryWithoutAFormatFails() throws IOException {

    String outputPath = createTempPath(""); // no extension to signal format

    HtsjdkReadsRddStorage htsjdkReadsRddStorage = HtsjdkReadsRddStorage.makeDefault(jsc);
    htsjdkReadsRddStorage.write(null, outputPath); // RDD is ignored, so OK to pass in null
  }

  @Test
  public void testOverwrite() throws IOException, URISyntaxException {
    String inputPath = getPath("1.bam");

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(128 * 1024).useNio(false);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);
    int expectedCount = AnySamTestUtil.countReads(inputPath);

    String outputPath = createTempPath(SamFormat.BAM.getExtension());
    Path p = Paths.get(URI.create(outputPath));
    Files.createFile(p); // create the file to check that overwrite works
    Assert.assertTrue(Files.exists(p));
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath);
    Assert.assertEquals(expectedCount, AnySamTestUtil.countReads(outputPath));
  }
}
