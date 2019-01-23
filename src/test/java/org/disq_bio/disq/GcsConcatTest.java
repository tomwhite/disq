/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018-2019 Disq contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.disq_bio.disq;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * See documentation about GCS compose at https://cloud.google.com/storage/docs/composite-objects.
 *
 * It might be possible to improve the way that composition is done, e.g.
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Merger.java#L782
 * Or hierarchical merging using a parallel executor to perform compose operations
 * in parallel. These need trying/benchmarking.
 */
public class GcsConcatTest {

  // limit is from https://cloud.google.com/storage/docs/composite-objects
  private static final int MAX_SOURCE_COMPONENTS_TO_COMPOSE = 32;

  private Storage storage;

  public GcsConcatTest() {
    this.storage = StorageOptions.getDefaultInstance().getService();
  }

  //@Test
  public void createSourceObjects() {
    for (int i = 0; i < 40; i++) {
      Blob blob = createBlobFromByteArray("gatk-tom-testdata", "concat/data/" + String.valueOf(i) + ".txt", String.valueOf(i));
      System.out.println(blob);
    }
  }

  //@Test
  public void composeTwo() {
    Blob blob = composeBlobs("gatk-tom-testdata", "concat/compose12.txt", "concat/data/1.txt", "concat/data/2.txt");
    System.out.println(blob);
  }

  @Test
  public void composeMore() {
    int n = 40;
    List<String> blobs = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      blobs.add("concat/data/" + String.valueOf(i) + ".txt");
    }
    Blob blob = composeBlobs("gatk-tom-testdata", "concat/compose0-" + String.valueOf(n) + ".txt", blobs);
    System.out.println(blob);
  }


  public Blob createBlobFromByteArray(String bucketName, String blobName, String content) {
    BlobId blobId = BlobId.of(bucketName, blobName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    return storage.create(blobInfo, content.getBytes(UTF_8));
  }

  public Blob composeBlobs(
      String bucketName, String blobName, String sourceBlob1, String sourceBlob2) {
    BlobId blobId = BlobId.of(bucketName, blobName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    Storage.ComposeRequest request =
        Storage.ComposeRequest.newBuilder()
            .setTarget(blobInfo)
            .addSource(sourceBlob1)
            .addSource(sourceBlob2)
            .build();
    return storage.compose(request);
  }

  public Blob composeBlobs(
      String bucketName, String blobName, List<String> sourceBlobs) {
    BlobId blobId = BlobId.of(bucketName, blobName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();

    if (sourceBlobs.size() <= MAX_SOURCE_COMPONENTS_TO_COMPOSE) {
      System.out.println("Composing " + sourceBlobs);
      Storage.ComposeRequest request =
          Storage.ComposeRequest.newBuilder()
              .setTarget(blobInfo)
              .addSource(sourceBlobs)
              .build();
      return storage.compose(request);
    } else {
      // first partition to create the target
      System.out.println("Composing " + sourceBlobs.subList(0, MAX_SOURCE_COMPONENTS_TO_COMPOSE));
      Storage.ComposeRequest request =
          Storage.ComposeRequest.newBuilder()
              .setTarget(blobInfo)
              .addSource(sourceBlobs.subList(0, MAX_SOURCE_COMPONENTS_TO_COMPOSE))
              .build();
      Blob blob = storage.compose(request);
      // subsequent partitions append to the target
      List<List<String>> partitions = Lists.partition(sourceBlobs.subList(MAX_SOURCE_COMPONENTS_TO_COMPOSE, sourceBlobs.size()), MAX_SOURCE_COMPONENTS_TO_COMPOSE - 1);
      for (List<String> partition : partitions) {
        System.out.println("Composing " + blobName + " and " + partition);
        request =
            Storage.ComposeRequest.newBuilder()
                .setTarget(blobInfo)
                .addSource(blobName) // include target to append to
                .addSource(partition)
                .build();
        blob = storage.compose(request);
      }
      return blob;
    }
  }

}
