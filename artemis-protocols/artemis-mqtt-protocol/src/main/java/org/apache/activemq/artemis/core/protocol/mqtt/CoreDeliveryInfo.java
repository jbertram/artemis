/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.mqtt;

import java.util.Objects;

import org.apache.activemq.artemis.api.core.SimpleString;

public class CoreDeliveryInfo {
   private long consumerId;
   // TODO should this use the PacketCorrelationKey?
   private long coreMessageId;
   private SimpleString address;

   public static CoreDeliveryInfo of(long consumerId, long coreMessageId, SimpleString address) {
      return new CoreDeliveryInfo(consumerId, coreMessageId, address);
   }

   private CoreDeliveryInfo(long consumerId, long coreMessageId, SimpleString address) {
      this.consumerId = consumerId;
      this.coreMessageId = coreMessageId;
      this.address = address;
   }

   public long getConsumerId() {
      return consumerId;
   }

   public long getCoreMessageId() {
      return coreMessageId;
   }

   public SimpleString getAddress() {
      return address;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof CoreDeliveryInfo other)) {
         return false;
      }
      return consumerId == other.consumerId &&
         coreMessageId == other.coreMessageId &&
         Objects.equals(address, other.address);
   }

   @Override
   public int hashCode() {
      return Objects.hash(consumerId, coreMessageId, address);
   }

   @Override
   public String toString() {
      return "CoreDeliveryInfo[" + "consumerId=" + consumerId + ", coreMessageId=" + coreMessageId + ", address=" + address + "]";
   }
}
