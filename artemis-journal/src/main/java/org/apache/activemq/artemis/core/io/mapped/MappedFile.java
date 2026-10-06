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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.util.DirectByteBufferReleaser;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.utils.PowerOf2Util;
import org.apache.activemq.artemis.utils.Env;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;

final class MappedFile implements AutoCloseable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int OS_PAGE_SIZE = Env.osPageSize();

   /*
    * Raw pointer arithmetic (PlatformDependent.directBufferAddress/copyMemory/setMemory) requires sun.misc.Unsafe.
    * Unlike DirectByteBufferReleaser.freeDirectBuffer, Netty's PlatformDependent does not fall back internally when
    * Unsafe is unavailable: it calls straight into Unsafe and would throw/NPE. So every raw-address operation below
    * is gated on HAS_UNSAFE, with a bounds-checked java.nio.ByteBuffer fallback (slower, but correct) for JDK 24+
    * without --sun-misc-unsafe-memory-access=allow, or any future JDK where Unsafe memory access is removed.
    */
   private static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();
   private static final byte[] ZEROS = new byte[OS_PAGE_SIZE];

   private final MappedByteBuffer buffer;
   private final FileChannel channel;
   private final long address;
   private final ByteBuf byteBufWrapper;
   private final ChannelBufferWrapper channelBufferWrapper;
   private int position;
   private int length;

   private MappedFile(FileChannel channel, MappedByteBuffer byteBuffer, int position, int length) throws IOException {
      this.channel = channel;
      this.buffer = byteBuffer;
      this.position = position;
      this.length = length;
      this.byteBufWrapper = Unpooled.wrappedBuffer(buffer);
      this.channelBufferWrapper = new ChannelBufferWrapper(this.byteBufWrapper, false);
      this.address = HAS_UNSAFE ? PlatformDependent.directBufferAddress(buffer) : 0;
   }

   public static MappedFile of(File file, int position, int capacity) throws IOException {
      final MappedByteBuffer buffer;
      final int length;
      final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
      length = (int) channel.size();
      if (length != capacity && length != 0) {
         if (logger.isDebugEnabled()) {
            logger.debug("Adjusting capacity to {} while it was {} on file {}", length, capacity, file);
         }
         capacity = length;
      }
      buffer = channel.map(FileChannel.MapMode.READ_WRITE, position, capacity);
      return new MappedFile(channel, buffer, 0, length);
   }

   public FileChannel channel() {
      return channel;
   }

   public MappedByteBuffer mapped() {
      return buffer;
   }

   public long address() {
      return this.address;
   }

   public void force() {
      this.buffer.force();
   }

   private void checkCapacity(int requiredCapacity) {
      if (requiredCapacity < 0 || requiredCapacity > buffer.capacity()) {
         throw new IllegalStateException("requiredCapacity must be >0 and <= " + buffer.capacity());
      }
   }

   /**
    * It is raw because it doesn't validate capacity through {@link #checkCapacity(int)}.
    */
   private void rawMovePositionAndLength(int position) {
      this.position = position;
      if (position > this.length) {
         this.length = position;
      }
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * Bytes are read starting at this file's current position, and then the position is updated with the number of bytes
    * actually read.
    */
   public int read(ByteBuffer dst, int dstStart, int dstLength) throws IOException {
      final int remaining = this.length - this.position;
      final int read = Math.min(remaining, dstLength);
      if (HAS_UNSAFE) {
         final long srcAddress = this.address + this.position;
         if (dst.isDirect()) {
            final long dstAddress = PlatformDependent.directBufferAddress(dst) + dstStart;
            PlatformDependent.copyMemory(srcAddress, dstAddress, read);
         } else {
            final byte[] dstArray = dst.array();
            PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, read);
         }
      } else {
         //bounds-checked bulk absolute transfer: works for both direct and heap dst, no raw address needed
         dst.put(dstStart, buffer, this.position, read);
      }
      this.position += read;
      return read;
   }

   /**
    * Writes an encoded sequence of bytes to this file from the given buffer.
    * <p>
    * Bytes are written starting at this file's current position,
    */
   public void write(EncodingSupport encodingSupport) throws IOException {
      final int encodedSize = encodingSupport.getEncodeSize();
      final int nextPosition = this.position + encodedSize;
      checkCapacity(nextPosition);
      this.byteBufWrapper.setIndex(this.position, this.position);
      encodingSupport.encode(this.channelBufferWrapper);
      rawMovePositionAndLength(nextPosition);
      assert (byteBufWrapper.writerIndex() == this.position);
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * Bytes are written starting at this file's current position,
    */
   public void write(ByteBuf src, int srcStart, int srcLength) throws IOException {
      final int nextPosition = this.position + srcLength;
      checkCapacity(nextPosition);
      if (HAS_UNSAFE) {
         final long destAddress = this.address + this.position;
         if (src.hasMemoryAddress()) {
            final long srcAddress = src.memoryAddress() + srcStart;
            PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
         } else if (src.hasArray()) {
            final byte[] srcArray = src.array();
            PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
         } else {
            throw new IllegalArgumentException("unsupported byte buffer");
         }
      } else {
         //ByteBuf.getBytes handles the direct/heap/composite split internally, no raw address needed
         //(transfers dst.remaining() bytes, hence sizing dup's remaining to exactly srcLength)
         final ByteBuffer dup = buffer.duplicate();
         dup.limit(nextPosition).position(this.position);
         src.getBytes(srcStart, dup);
      }
      rawMovePositionAndLength(nextPosition);
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * Bytes are written starting at this file's current position,
    */
   public void write(ByteBuffer src, int srcStart, int srcLength) throws IOException {
      final int nextPosition = this.position + srcLength;
      checkCapacity(nextPosition);
      if (HAS_UNSAFE) {
         final long destAddress = this.address + this.position;
         if (src.isDirect()) {
            final long srcAddress = PlatformDependent.directBufferAddress(src) + srcStart;
            PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
         } else {
            final byte[] srcArray = src.array();
            PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
         }
      } else {
         //bounds-checked bulk absolute transfer: works for both direct and heap src, no raw address needed
         buffer.put(this.position, src, srcStart, srcLength);
      }
      rawMovePositionAndLength(nextPosition);
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * Bytes are written starting at this file's current position,
    */
   public void zeros(int position, final int count) throws IOException {
      checkCapacity(position + count);
      if (HAS_UNSAFE) {
         //zeroes memory in reverse direction in OS_PAGE_SIZE batches
         //to gain sympathy by the page cache LRU policy
         final long start = this.address + position;
         final long end = start + count;
         int toZeros = count;
         final long lastGap = (int) (end & (OS_PAGE_SIZE - 1));
         final long lastStartPage = end - lastGap;
         long lastZeroed = end;
         if (start <= lastStartPage) {
            if (lastGap > 0) {
               PlatformDependent.setMemory(lastStartPage, lastGap, (byte) 0);
               lastZeroed = lastStartPage;
               toZeros -= lastGap;
            }
         }
         //any that will enter has lastZeroed OS page aligned
         while (toZeros >= OS_PAGE_SIZE) {
            assert PowerOf2Util.isAligned(lastZeroed, OS_PAGE_SIZE);/**/
            final long startPage = lastZeroed - OS_PAGE_SIZE;
            PlatformDependent.setMemory(startPage, OS_PAGE_SIZE, (byte) 0);
            lastZeroed = startPage;
            toZeros -= OS_PAGE_SIZE;
         }
         //there is anything left in the first OS page?
         if (toZeros > 0) {
            PlatformDependent.setMemory(start, toZeros, (byte) 0);
         }
      } else {
         //bounds-checked bulk absolute put from a shared zero-filled array, no raw address needed
         int pos = position;
         int remaining = count;
         while (remaining > 0) {
            final int chunk = Math.min(remaining, ZEROS.length);
            buffer.put(pos, ZEROS, 0, chunk);
            pos += chunk;
            remaining -= chunk;
         }
      }
      //do not move this.position: only this.length can be changed
      position += count;
      if (position > this.length) {
         this.length = position;
      }
   }

   public int position() {
      return this.position;
   }

   public void position(int position) {
      checkCapacity(position);
      this.position = position;
   }

   public long length() {
      return this.length;
   }

   @Override
   public void close() {
      try {
         channel.close();
      } catch (IOException e) {
         throw new IllegalStateException(e);
      } finally {
         //unmap in a deterministic way when possible: falls back to GC-triggered cleanup if native freeing is unavailable
         DirectByteBufferReleaser.freeDirectBuffer(this.buffer);
      }
   }
}
