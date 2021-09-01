/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.examples;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.security.UserGroupInformation;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A mini demo program that access Ozone Volume/Bucket/Key via Ozone RPC
 * with minimal dependency on classpath/configuration stuff.
 * It has been tested with secure, non-secure, HA and non-HA Ozone clusters.
 */
public class OzoneRpc {
  private static long numFiles;
  private static int chunkSize;
  private static long fileSizeInBytes = 128000000;
  private static long bufferSizeInBytes = 4000000;
  private static String[] storageDir = new String[]{"/data/ratis/", "/data1/ratis/", "/data2/ratis/", "/data3/ratis/",
      "/data4/ratis/", "/data5/ratis/", "/data6/ratis/", "/data7/ratis/", "/data8/ratis/", "/data9/ratis/",
      "/data10/ratis/", "/data11/ratis/"};

  private static void createDirs() throws IOException {
    for (String dir : storageDir) {
      Files.createDirectory(new File(dir).toPath());
    }
  }

  public static String getPath(String fileName) {
    int hash = fileName.hashCode() % storageDir.length;
    return new File(storageDir[Math.abs(hash)], fileName).getAbsolutePath();
  }

  protected static void dropCache() throws InterruptedException, IOException {
    String[] cmds = {"/bin/sh","-c","echo 3 > /proc/sys/vm/drop_caches"};
    Process pro = Runtime.getRuntime().exec(cmds);
    pro.waitFor();
  }

  private static CompletableFuture<Long> writeFileAsync(String path, ExecutorService executor) {
    final CompletableFuture<Long> future = new CompletableFuture<>();
    CompletableFuture.supplyAsync(() -> {
      try {
        future.complete(
            writeFile(path, fileSizeInBytes, bufferSizeInBytes, new Random().nextInt(127) + 1));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      return future;
    }, executor);
    return future;
  }

  protected static List<String> generateFiles(ExecutorService executor) {
    UUID uuid = UUID.randomUUID();
    List<String> paths = new ArrayList<>();
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String path = getPath("file-" + uuid + "-" + i);
      paths.add(path);
      futures.add(writeFileAsync(path, executor));
    }

    for (int i = 0; i < futures.size(); i ++) {
      long size = futures.get(i).join();
      if (size != fileSizeInBytes) {
        System.err.println("Error: path:" + paths.get(i) + " write:" + size +
            " mismatch expected size:" + fileSizeInBytes);
      }
    }

    return paths;
  }

  protected static long writeFile(String path, long fileSize, long bufferSize, int random) throws IOException {
    RandomAccessFile raf = null;
    long offset = 0;
    try {
      raf = new RandomAccessFile(path, "rw");
      while (offset < fileSize) {
        final long remaining = fileSize - offset;
        final long chunkSize = Math.min(remaining, bufferSize);
        byte[] buffer = new byte[(int)chunkSize];
        for (int i = 0; i < chunkSize; i ++) {
          buffer[i]= (byte) (i % random);
        }
        raf.write(buffer);
        offset += chunkSize;
      }
    } finally {
      if (raf != null) {
        raf.close();
      }
    }
    return offset;
  }

  private static Map<String, CompletableFuture<Boolean>> writeByHeapByteBuffer(
      List<String> paths, List<OzoneDataStreamOutput> outs, ExecutorService executor) {
    Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();

    for(int i = 0; i < paths.size(); i ++) {

      String path = paths.get(i);
      OzoneDataStreamOutput out = outs.get(i);
      final CompletableFuture<Boolean> future = new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        File file = new File(path);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");) {
          FileChannel ch = raf.getChannel();
          long len = raf.length();
          long off = 0;
          while (len > 0) {
            long writeLen = Math.min(len, chunkSize);
            ByteBuffer segment =
                ch.map(FileChannel.MapMode.READ_ONLY, off, writeLen);
            ByteBuf buf = Unpooled.wrappedBuffer(segment);
            out.write(buf);
            off += writeLen;
            len -= writeLen;
          }
          out.close();
          future.complete(true);
        } catch (Throwable e) {
          future.complete(false);
        }

        return future;
      }, executor);

      fileMap.put(path, future);
    }

    return fileMap;
  }
//private static Map<String, CompletableFuture<Boolean>> writeByHeapByteBuffer(
//    List<String> paths, List<OzoneDataStreamOutput> outs, ExecutorService executor) {
//  Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();
//
//  for(int i = 0; i < paths.size(); i ++) {
//
//    String path = paths.get(i);
//    OzoneDataStreamOutput out = outs.get(i);
//    final CompletableFuture<Boolean> future = new CompletableFuture<>();
//    CompletableFuture.supplyAsync(() -> {
//      File file = new File(path);
//      try (RandomAccessFile raf = new RandomAccessFile(file, "r");) {
//        FileChannel in = raf.getChannel();
//        for (long offset = 0L; offset < file.length(); ) {
//          ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(chunkSize);
//            int bytesRead = buf.writeBytes(in, chunkSize);
//            out.write(buf);
//            offset +=bytesRead;
//            if (buf != null && bytesRead>0) {
//              buf.release();
//            }
//        }
//        out.close();
//        future.complete(true);
//      } catch (Throwable e) {
//        future.complete(false);
//      }
//
//      return future;
//    }, executor);
//
//    fileMap.put(path, future);
//  }
//
//  return fileMap;
//}

  static OzoneClient getOzoneClient(boolean secure) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    // TODO: If you don't have OM HA configured, change the following as appropriate.
    conf.set("ozone.om.address", "9.29.173.57:9862");
    return OzoneClientFactory.getRpcClient(conf);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Ozone Rpc Demo Begin.");
    OzoneClient ozoneClient = null;

    try {
      String testVolumeName = args[0];
      String testBucketName = args[1];
      numFiles = Integer.parseInt(args[2]);
      chunkSize = Integer.parseInt(args[3]);
      System.out.println("numFiles:" + numFiles);
      System.out.println("chunkSize:" + chunkSize);
      createDirs();
      final ExecutorService executor = Executors.newFixedThreadPool(1000);

      List<String> paths = generateFiles(executor);
      dropCache();
      // Get an Ozone RPC Client.
      ozoneClient = getOzoneClient(false);

      // An Ozone ObjectStore instance is the entry point to access Ozone.
      ObjectStore store = ozoneClient.getObjectStore();

      // Create volume with random name.
      store.createVolume(testVolumeName);
      OzoneVolume volume = store.getVolume(testVolumeName);
      System.out.println("Volume " + testVolumeName + " created.");

      // Create bucket with random name.
      volume.createBucket(testBucketName);
      OzoneBucket bucket = volume.getBucket(testBucketName);
      System.out.println("Bucket " + testBucketName + " created.");

      List<OzoneDataStreamOutput> outs = new ArrayList<OzoneDataStreamOutput>();
      ReplicationConfig config =
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);
      for (int i = 0; i < paths.size(); i ++) {
        OzoneDataStreamOutput out = bucket.createStreamKey("ozonekey_" + i, 128000000,
            config, new HashMap<>());
        outs.add(out);
      }

      long start = System.currentTimeMillis();
      // Write key with random name.
      Map<String, CompletableFuture<Boolean>> map = writeByHeapByteBuffer(paths, outs, executor);

      for (String path : map.keySet()) {
        CompletableFuture<Boolean> future = map.get(path);
        if (!future.join().booleanValue()) {
          System.err.println("Error: path:" + path + " write fail");
        }
      }

      long end = System.currentTimeMillis();
      System.err.println("cost:" + (end - start) + " xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    } catch (Exception e) {
      System.err.println(e);
    } finally {
      // Ensure Ozone client resource is safely closed at the end.
      if (ozoneClient != null) {
        ozoneClient.close();
      }
    }
    System.out.println("Ozone Rpc Demo End.");
    System.exit(0);
  }

  private static String getRandomString(int len) {
    return RandomStringUtils.randomAlphanumeric(len).toLowerCase();
  }
}
