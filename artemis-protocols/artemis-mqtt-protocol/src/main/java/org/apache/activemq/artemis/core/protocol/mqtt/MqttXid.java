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
package org.apache.activemq.artemis.core.protocol.mqtt;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.core.transaction.impl.XidImpl;

/**
 * MQTT QoS2 Xid implementation that encodes MQTT client ID and packet ID.
 */
public class MqttXid extends XidImpl {

   /**
    * Format ID for MQTT QoS2 transactions (ASCII "MQT2" = 0x4D515432)
    */
   public static final int MQTT_QOS2_FORMAT_ID = 0x4D515432;
   private static final byte[] MQTT_BRANCH_QUALIFIER = "mqtt".getBytes(StandardCharsets.UTF_8);

   private final String clientId;
   private final int packetId;

   /**
    * Create a new MQTT QoS2 Xid from client ID and packet ID.
    *
    * @param clientId MQTT client ID
    * @param packetId MQTT packet ID
    */
   public static MqttXid of(String clientId, int packetId) {
      return new MqttXid(clientId, packetId);
   }

   private MqttXid(String clientId, int packetId) {
      super(MQTT_BRANCH_QUALIFIER, MQTT_QOS2_FORMAT_ID, encodeGlobalTransactionId(clientId, packetId));
      this.clientId = clientId;
      this.packetId = packetId;
   }

   /**
    * Get the MQTT client ID from this Xid.
    *
    * @return MQTT client ID
    */
   public String getClientId() {
      return clientId;
   }

   /**
    * Get the MQTT packet ID from this Xid.
    *
    * @return MQTT packet ID
    */
   public int getPacketId() {
      return packetId;
   }

   /**
    * Check if an Xid is an MQTT QoS2 transaction.
    *
    * @param xid Xid to check
    * @return true if this is an MQTT QoS2 Xid
    */
   public static boolean isMqttXid(Xid xid) {
      return xid != null && xid.getFormatId() == MQTT_QOS2_FORMAT_ID;
   }

   /**
    * Decode an MQTT QoS2 Xid from a generic Xid.
    *
    * @param xid Xid to decode
    * @return decoded MQTTQoS2Xid
    * @throws IllegalArgumentException if the Xid is not an MQTT QoS2 transaction
    */
   public static MqttXid fromXid(Xid xid) {
      if (!isMqttXid(xid)) {
         throw new IllegalArgumentException("Not an MQTT QoS2 Xid: formatId=" + xid.getFormatId());
      }

      byte[] globalTxId = xid.getGlobalTransactionId();
      ByteBuffer buffer = ByteBuffer.wrap(globalTxId);

      int clientIdLength = buffer.getInt();
      byte[] clientIdBytes = new byte[clientIdLength];
      buffer.get(clientIdBytes);
      String clientId = new String(clientIdBytes, StandardCharsets.UTF_8);

      int packetId = buffer.getInt();

      return new MqttXid(clientId, packetId);
   }

   /**
    * Encode client ID and packet ID into globalTransactionId byte array. Format: [clientId length (4 bytes)][clientId
    * UTF-8 bytes][packetId (4 bytes)]
    *
    * @param clientId MQTT client ID
    * @param packetId MQTT packet ID
    * @return encoded byte array
    */
   private static byte[] encodeGlobalTransactionId(String clientId, int packetId) {
      byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.allocate(4 + clientIdBytes.length + 4);
      buffer.putInt(clientIdBytes.length);
      buffer.put(clientIdBytes);
      buffer.putInt(packetId);
      return buffer.array();
   }

   @Override
   public String toString() {
      return "MqttXid[clientId=" + clientId + ", packetId=" + packetId + ", base64=" + toBase64String(this) + "]";
   }
}
