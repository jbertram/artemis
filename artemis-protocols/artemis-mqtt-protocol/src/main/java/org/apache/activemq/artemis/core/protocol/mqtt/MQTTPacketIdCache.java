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

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.ByteUtil;

public class MQTTPacketIdCache {

   private DuplicateIDCache cache;
   private SimpleString cacheName;
   private MQTTSession session;
   private SimpleString subName;

   public MQTTPacketIdCache(MQTTSession session, SimpleString subName) {
      this.session = session;
      this.subName = subName;
      this.cacheName = getCacheName(session.getServer().getInternalNamingPrefix(), session.getState().getClientId(), subName);
   }

   public boolean exists(int packetId) {
      init();
      return cache.contains(ByteUtil.intToBytes(packetId));
   }

   public void add(int packetId, Transaction tx) throws Exception {
      init();
      cache.addToCache(ByteUtil.intToBytes(packetId), tx);
   }

   private void init() {
      if (cache == null) {
         cache = session.getServer().getPostOffice().getDuplicateIDCache(cacheName, MQTTUtil.TWO_BYTE_INT_MAX);
      }
   }

   public boolean remove(int packetId) throws Exception {
      if (cache == null) {
         return false;
      } else {
         return cache.deleteFromCache(ByteUtil.intToBytes(packetId));
      }
   }

   public void clear() throws Exception {
      if (cache != null) {
         session.getServer().getPostOffice().deleteDuplicateCache(cacheName);
         cache = null;
      }
   }

   public int size() {
      return cache.getMap().size();
   }

   public List<Integer> getPacketIds() {
      init();
      List<Integer> result = new ArrayList<>();
      for (Pair<byte[], Long> entry : cache.getMap()) {
         result.add(ByteUtil.bytesToInt(entry.getA()));
      }
      return result;
   }

   public static SimpleString getCacheName(String prefix, String clientId, SimpleString subName) {
      return SimpleString.of(prefix).concat("mqtt.qos2.").concat(subName).concat('.').concat(clientId);
   }
}
