/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.io.mapped;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the mapped journal's read/write/zero-fill paths when {@code sun.misc.Unsafe} is unavailable.
 * {@link MappedFile} normally drives these through raw pointer arithmetic
 * ({@code PlatformDependent.directBufferAddress/copyMemory/setMemory}), which requires Unsafe and has no internal
 * fallback in Netty: without Unsafe those calls fail outright. In that case {@code MappedFile} must fall back to
 * bounds-checked {@link java.nio.ByteBuffer} bulk transfers instead.
 * <p>
 * On JDK 24+ at runtime, by default Netty avoids {@code sun.misc.Unsafe} ({@code hasUnsafe=false}) unless the JVM is
 * started with {@code --sun-misc-unsafe-memory-access=allow}. The same no-Unsafe path is also reached when Unsafe is
 * explicitly disabled ({@code -Dio.netty.noUnsafe=true}) or on a future JDK where {@code sun.misc.Unsafe} is gone
 * entirely.
 */
public class MappedFileNoUnsafeTest {

   private static final int CAPACITY = 64 * 1024;

   // runs in the spawned child JVM (started with -Dio.netty.noUnsafe=true to force Netty's no-Unsafe path)
   public static void main(String[] args) {
      try {
         final File dir = Files.createTempDirectory("MappedFileNoUnsafeTest").toFile();
         dir.deleteOnExit();
         final MappedSequentialFileFactory factory = new MappedSequentialFileFactory(dir, CAPACITY, false, 0, 0, null);
         factory.start();
         final SequentialFile file = factory.createSequentialFile("mapped.dat");
         file.open();

         // exercises MappedFile.zeros() fallback
         file.fill(CAPACITY);
         file.position(0);
         final ByteBuffer readBack = ByteBuffer.allocate(CAPACITY);
         file.read(readBack);
         for (int i = 0; i < CAPACITY; i++) {
            if (readBack.get(i) != 0) {
               throw new AssertionError("expected zero at index " + i);
            }
         }

         // exercises MappedFile.write(ByteBuffer,...)/read(ByteBuffer,...) fallback, for both direct and heap buffers
         file.position(0);
         final byte[] directPayload = payload(1024, (byte) 1);
         final ByteBuffer directSrc = ByteBuffer.allocateDirect(directPayload.length);
         directSrc.put(directPayload).flip();
         file.writeDirect(directSrc, true);

         final byte[] heapPayload = payload(1024, (byte) 2);
         file.writeDirect(ByteBuffer.wrap(heapPayload), true);

         file.position(0);
         final ByteBuffer directDst = ByteBuffer.allocateDirect(directPayload.length);
         file.read(directDst);
         checkEquals(directPayload, directDst);

         final ByteBuffer heapDst = ByteBuffer.allocate(heapPayload.length);
         file.read(heapDst);
         checkEquals(heapPayload, heapDst);

         // exercises MappedFile.write(ByteBuf,...) fallback via ActiveMQBuffer
         file.position(0);
         final byte[] buffPayload = payload(512, (byte) 3);
         final ActiveMQBuffer activeMQBuffer = ActiveMQBuffers.wrappedBuffer(buffPayload);
         file.write(activeMQBuffer, true);

         file.position(0);
         final ByteBuffer buffDst = ByteBuffer.allocate(buffPayload.length);
         file.read(buffDst);
         checkEquals(buffPayload, buffDst);

         file.close();
         factory.stop();
         System.exit(0);
      } catch (Throwable e) {
         e.printStackTrace();
         System.exit(100);
      }
   }

   private static byte[] payload(int length, byte fill) {
      final byte[] bytes = new byte[length];
      Arrays.fill(bytes, fill);
      return bytes;
   }

   // SequentialFile.read(ByteBuffer) already flips the buffer, so this checks it in its post-read state
   private static void checkEquals(byte[] expected, ByteBuffer actual) {
      for (int i = 0; i < expected.length; i++) {
         if (actual.get(i) != expected[i]) {
            throw new AssertionError("mismatch at index " + i + ": expected " + expected[i] + " but got " + actual.get(i));
         }
      }
   }

   @Test
   public void readWriteZerosWithoutUnsafe() throws Exception {
      final String javaPath = new File(new File(System.getProperty("java.home"), "bin"), "java").getAbsolutePath();
      final List<String> command = new ArrayList<>();
      command.add(javaPath);
      command.add("-cp");
      command.add(SpawnedVMSupport.getClassPath());
      // force Netty's no-Unsafe path (hasUnsafe=false); in that configuration PlatformDependent.directBufferAddress/
      // copyMemory/setMemory are unusable and MappedFile must fall back to bounds-checked ByteBuffer bulk transfers
      command.add("-Dio.netty.noUnsafe=true");
      command.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir", "./tmp"));
      command.add(MappedFileNoUnsafeTest.class.getName());

      final ProcessBuilder builder = new ProcessBuilder(command);
      builder.inheritIO();
      final Process process = builder.start();
      assertEquals(0, process.waitFor(), "mapped journal read/write/zero-fill must work without Unsafe");
   }
}
