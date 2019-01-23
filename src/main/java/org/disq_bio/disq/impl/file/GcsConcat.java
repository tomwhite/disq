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
package org.disq_bio.disq.impl.file;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.CloudStoragePath;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class GcsConcat {

  private static final Logger logger = LoggerFactory.getLogger(GcsConcat.class);

  // limit is from https://cloud.google.com/storage/docs/composite-objects
  private static final int MAX_SOURCE_COMPONENTS_TO_COMPOSE = 32;

  public static void concat(List<String> parts, String path) {
    Storage storage = StorageOptions.getDefaultInstance().getService();

    List<String> sourceBlobs = parts
        .stream()
        .map(GcsConcat::pathToBlobName)
        .collect(Collectors.toList());

    composeBlobs(storage, sourceBlobs, pathToBlobId(path));
  }

  private static BlobId pathToBlobId(String path) {
    Path gsPath = NioFileSystemWrapper.asPath(path);
    if (!(gsPath instanceof CloudStoragePath)) {
      throw new IllegalArgumentException("Path must be a CloudStoragePath (gs://)");
    }
    CloudStoragePath cloudPath = (CloudStoragePath) gsPath;
    return BlobId.of(cloudPath.bucket(), cloudPath.toRealPath().toString());
  }

  private static String pathToBlobName(String path) {
    return pathToBlobId(path).getName();
  }

  private static Blob composeBlobs(Storage storage, List<String> sourceBlobs, BlobId blobId) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();

    if (sourceBlobs.size() <= MAX_SOURCE_COMPONENTS_TO_COMPOSE) {
      logger.info("Composing blobs {} to {}", sourceBlobs, blobInfo);
      Storage.ComposeRequest request =
          Storage.ComposeRequest.newBuilder()
              .setTarget(blobInfo)
              .addSource(sourceBlobs)
              .build();
      return storage.compose(request);
    } else {
      // first partition to create the target
      List<String> firstSourceBlobs = sourceBlobs.subList(0, MAX_SOURCE_COMPONENTS_TO_COMPOSE);
      logger.info("Composing blobs {} to {}", firstSourceBlobs, blobInfo);
      Storage.ComposeRequest request =
          Storage.ComposeRequest.newBuilder()
              .setTarget(blobInfo)
              .addSource(firstSourceBlobs)
              .build();
      Blob blob = storage.compose(request);
      // subsequent partitions append to the target
      List<List<String>> partitions = Lists.partition(sourceBlobs.subList(MAX_SOURCE_COMPONENTS_TO_COMPOSE, sourceBlobs.size()), MAX_SOURCE_COMPONENTS_TO_COMPOSE - 1);
      for (List<String> partition : partitions) {
        logger.info("Composing blobs {} with {}", partition, blobId.getName());
        request =
            Storage.ComposeRequest.newBuilder()
                .setTarget(blobInfo)
                .addSource(blobId.getName()) // include target to append to
                .addSource(partition)
                .build();
        blob = storage.compose(request);
      }
      return blob;
    }
  }
}
