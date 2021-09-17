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
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A mini demo program that access Ozone Volume/Bucket/Key via Ozone RPC
 * with minimal dependency on classpath/configuration stuff.
 * It has been tested with secure, non-secure, HA and non-HA Ozone clusters.
 */
public class OzoneRpc {
  private static boolean disableChecksum = false;
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
            writeFileFast(path, fileSizeInBytes));
            //writeFile(path, fileSizeInBytes, bufferSizeInBytes, new Random().nextInt(127) + 1));
      } catch (IOException e) {
        future.completeExceptionally(e);
      } catch (InterruptedException e) {
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

  protected static long writeFileFast(String path, long fileSize) throws InterruptedException, IOException {
    long mbytes = (fileSize >> 20) + 1;

    String[] cmd = {"/bin/bash", "-c", "dd if=<(openssl enc -aes-256-ctr -pass pass:\"$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64)\" -nosalt < /dev/zero) of=" + path + " bs=1M count=" + mbytes + " iflag=fullblock"};
    Process ps = Runtime.getRuntime().exec(cmd);
    ps.waitFor();

    String[] cmd2 = {"/usr/bin/truncate", "--size=" + fileSize, "path"};
    Process ps2 = Runtime.getRuntime().exec(cmd2);
    ps2.waitFor();

    return fileSize;
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
          List<String> paths, List<OzoneOutputStream> outs, ExecutorService executor) {
    Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();

    for(int i = 0; i < paths.size(); i ++) {
      String path = paths.get(i);
      OzoneOutputStream out = outs.get(i);
      final CompletableFuture<Boolean> future = new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        File file = new File(path);
        try (FileInputStream fis = new FileInputStream(file)) {
          int bytesToRead = (int)bufferSizeInBytes;
          byte[] buffer = new byte[bytesToRead];
          long offset = 0L;
          while(fis.read(buffer, 0, bytesToRead) > 0) {
            out.write(buffer);
            offset += bytesToRead;
            bytesToRead = (int) Math.min(fileSizeInBytes - offset, bufferSizeInBytes);
            if (bytesToRead > 0) {
              buffer = new byte[bytesToRead];
            }
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

  private static Map<String, CompletableFuture<Boolean>> writeByFileRegion(
          List<String> paths, List<OzoneDataStreamOutput> outs, ExecutorService executor) {
    Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();

    for(int i = 0; i < paths.size(); i ++) {

      String path = paths.get(i);
      OzoneDataStreamOutput out = outs.get(i);
      final CompletableFuture<Boolean> future = new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        File file = new File(path);
        try {
          out.write(file);
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

  private static Map<String, CompletableFuture<Boolean>> writeByMappedByteBuffer(
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
            ByteBuffer bb = ch.map(FileChannel.MapMode.READ_ONLY, off, writeLen);
            out.write(bb);
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

  static OzoneClient getOzoneClient(boolean secure) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    // TODO: If you don't have OM HA configured, change the following as appropriate.
    conf.set("ozone.om.address", "9.29.173.57:9862");
    if (disableChecksum)
      conf.set("ozone.client.checksum.type", "NONE");
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
      if (args.length > 4)
        fileSizeInBytes = Long.parseLong(args[4]);
      if (args.length > 5) {
        disableChecksum = (Integer.parseInt(args[5]) != 0);
      }
      System.out.println("numFiles:" + numFiles);
      System.out.println("chunkSize:" + chunkSize);
      System.out.println("fileSize:" + fileSizeInBytes);
      System.out.println("checksum:" + (disableChecksum ? "NONE" : "DEFAULT"));
      createDirs();
      final ExecutorService executor = Executors.newFixedThreadPool(1000);
      final boolean streamApi = (chunkSize != 0);
      final boolean byByteBuffer = (chunkSize > 0);

      if (streamApi) {
        System.out.println("=== using " + (byByteBuffer ? "ByteBuffer" : "FileRegion") + " stream api ===");
      } else {
        System.out.println("=== using async api ===");
      }

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

      ReplicationConfig config =
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

      List<OzoneDataStreamOutput> streamOuts = new ArrayList<>();
      List<OzoneOutputStream> asyncOuts = new ArrayList<>();
      if (streamApi) {
        for (int i = 0; i < paths.size(); i++) {
          OzoneDataStreamOutput out = bucket.createStreamKey("ozonekey_" + i, 128000000,
              config, new HashMap<>());
          streamOuts.add(out);
        }
      } else {
        for (int i = 0; i < paths.size(); i++) {
          OzoneOutputStream out = bucket.createKey("ozonekey_" + i, 128000000,
              config, new HashMap<>());
          asyncOuts.add(out);
        }
      }


      // wait for sync signal
      System.out.println("=== input a new line to start the test ===");
      System.out.print(">>> ");
      new Scanner(System.in).nextLine();

      long start = System.currentTimeMillis();
      // Write key with random name.
      Map<String, CompletableFuture<Boolean>> map;
      if (streamApi) {
        map = byByteBuffer ? writeByMappedByteBuffer(paths, streamOuts, executor)
                           : writeByFileRegion(paths, streamOuts, executor);
      } else {
        map = writeByHeapByteBuffer(paths, asyncOuts, executor);
      }

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
