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
package org.apache.activemq.artemis.tests.integration.mqtt5;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTPacketIdCache;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for QoS 2 protocol resiliency with client reconnections and broker restarts.
 * <p>
 * QoS 2 Protocol Flow (broker sending to consumer):
 * <ol>
 * <li>Broker sends PUBLISH (QoS=2)</li>
 * <li>Consumer sends PUBREC</li>
 * <li>Broker sends PUBREL</li>
 * <li>Consumer sends PUBCOMP</li>
 * </ol>
 * These tests verify that the protocol maintains exactly-once delivery semantics when the broker is stopped and
 * restarted at each stage of the QoS 2 flow.
 */
public class QoS2SubscriberResiliencyTest extends MQTT5TestSupport {

   protected static final long DEFAULT_TIMEOUT_SEC = 10;

   @Override
   public boolean isProtocolLoggingEnabled() {
      return true;
   }

   /**
    * Verifies that the broker will re-use the same packet ID if it sends a PUBLISH but fails to receive the
    * corresponding PUBREC.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartBeforePubRecSent() throws Exception {
      testQoS2FailureBeforePubRecSent(true);
   }

   /**
    * Same test as {@link testQoS2BrokerRestartBeforePubRecSent} but disconnecting the client instead of restarting the
    * broker.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2ClientDisconnectBeforePubRecSent() throws Exception {
      testQoS2FailureBeforePubRecSent(false);
   }

   public void testQoS2FailureBeforePubRecSent(boolean restart) throws Exception {
      final String TOPIC = "test/resiliency";
      final String SUBSCRIBER_CLIENT_ID = "subscriber";
      final String PUBLISHER_CLIENT_ID = "publisher";
      final CountDownLatch pubRecLatch = new CountDownLatch(1);
      AtomicInteger pubRecCount = new AtomicInteger(0);
      final CountDownLatch stopLatch = new CountDownLatch(1);
      final CountDownLatch pubCompLatch = new CountDownLatch(1);

      // Set up interceptor to block the *second* incoming PUBREC.
      // We allow 1 PUBREC so the packet ID goes up to 2 for testing.
      MQTTInterceptor pubRecInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBREC && pubRecCount.incrementAndGet() > 1) {
            pubRecLatch.countDown();
            logger.info("Blocking incoming {}", packet.fixedHeader().messageType());
            try {
               stopLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
            return false;
         }
         logger.info("Allowing incoming {}", packet.fixedHeader().messageType());
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(pubRecInterceptor);

      // Consumer with persistent session
      MqttClient subscriber = createPahoClient(SUBSCRIBER_CLIENT_ID);
      subscriber.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            logger.info("messageArrived({}, {})", topic, message);
         }
      });
      MqttConnectionOptions subscriberOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      subscriber.connect(subscriberOptions);
      subscriber.subscribe(TOPIC, EXACTLY_ONCE);

      // Producer
      MqttClient producer = createPahoClient(PUBLISHER_CLIENT_ID);
      producer.connect();

      // Send 2 messages to ensure the packet ID is preserved by the broker.
      // If we just send 1 message it won't be clear if the broker just started generating packet IDs from scratch.
      producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);
      producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);

      Wait.assertEquals(0, () -> getPubCacheSize(PUBLISHER_CLIENT_ID));

      producer.disconnect();
      producer.close();

      assertNull(getPubCache(PUBLISHER_CLIENT_ID));

      assertTrue(pubRecLatch.await(5, TimeUnit.SECONDS));

      stopLatch.countDown();
      if (restart) {
         server.stop();
         waitForServerToStop(server);
         server.start();
         waitForServerToStart(server);
      } else {
         server.getRemotingService().clearInterceptors();
         server.getActiveMQServerControl().closeConnectionWithID(server.getActiveMQServerControl().listConnectionIDs()[0]);
      }

      assertTrue(getProtocolManager().getStateManager().qos2PacketIdCorrelationExists(SUBSCRIBER_CLIENT_ID, 2));

      MQTTInterceptor pubCompInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompLatch.countDown();
         }
         return true;
      };
      CountDownLatch packetIdLatch = new CountDownLatch(1);
      server.getRemotingService().addIncomingInterceptor(pubCompInterceptor);

      MQTTInterceptor pubInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            if (((MqttPublishMessage)packet).variableHeader().packetId() == 2) {
               packetIdLatch.countDown();
            }
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(pubInterceptor);

      Wait.waitFor(() -> {
         try {
            subscriber.reconnect();
            return true;
         } catch (MqttException e) {
            return false;
         }
      });

      assertTrue(packetIdLatch.await(5, TimeUnit.SECONDS), "Didn't find a PUBLISH with the expected packet id");
      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));

      Wait.assertEquals(0L, () -> getSubscriptionQueue(TOPIC, SUBSCRIBER_CLIENT_ID).getMessageCount(), 500, 25);

      assertEquals(0, getProtocolManager().getStateManager().getQos2PacketIdCorrelationSize(SUBSCRIBER_CLIENT_ID));
      assertEquals(0, getSubCacheSize(SUBSCRIBER_CLIENT_ID));

      subscriber.disconnect();

      // connect again to clean the session which will completely remove the cache from memory and disk
      subscriber.connect(new MqttConnectionOptionsBuilder().cleanStart(true).sessionExpiryInterval(0L).build());
      assertNull(getSubCache(SUBSCRIBER_CLIENT_ID));
      subscriber.disconnect();
      subscriber.close();
   }

   /**
    * Verifies that the broker correctly completes the QoS 2 flow if it receives the PUBREC but the corresponding
    * PUBREL is not sent to the consumer before the broker restarts.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartAfterPubRecSent() throws Exception {
      testQoS2FailureAfterPubRecSent(true);
   }

   /**
    * Same test as {@link testQoS2BrokerRestartAfterPubRecSent} but disconnecting the client instead of restarting the
    * broker.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2ClientDisconnectAfterPubRecSent() throws Exception {
      testQoS2FailureAfterPubRecSent(false);
   }

   public void testQoS2FailureAfterPubRecSent(boolean restart) throws Exception {
      final String TOPIC = "test/resiliency";
      final String SUBSCRIBER_CLIENT_ID = "subscriber";
      final String PUBLISHER_CLIENT_ID = "publisher";
      final CountDownLatch pubRelLatch = new CountDownLatch(1);
      final CountDownLatch pubCompLatch = new CountDownLatch(1);

      // Block the outgoing PUBREL so the broker has processed PUBREC but the consumer never receives PUBREL
      MQTTInterceptor pubRelInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            pubRelLatch.countDown();
            logger.info("Blocking outgoing {}", packet.fixedHeader().messageType());
            return false;
         }
         logger.info("Allowing outgoing {}", packet.fixedHeader().messageType());
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(pubRelInterceptor);

      // Consumer with persistent session
      MqttClient subscriber = createPahoClient(SUBSCRIBER_CLIENT_ID);
      subscriber.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            logger.info("messageArrived({}, {})", topic, message);
         }
      });
      MqttConnectionOptions subscriberOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      subscriber.connect(subscriberOptions);
      subscriber.subscribe(TOPIC, EXACTLY_ONCE);

      // Producer
      MqttClient producer = createPahoClient(PUBLISHER_CLIENT_ID);
      producer.connect();

      producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);

      Wait.assertEquals(0, () -> getPubCacheSize(PUBLISHER_CLIENT_ID));

      producer.disconnect();
      producer.close();

      assertNull(getPubCache(PUBLISHER_CLIENT_ID));

      assertTrue(pubRelLatch.await(5, TimeUnit.SECONDS));

      if (restart) {
         server.stop();
         waitForServerToStop(server);
         server.start();
         waitForServerToStart(server);
      } else {
         server.getRemotingService().clearInterceptors();
         server.getActiveMQServerControl().closeConnectionWithID(server.getActiveMQServerControl().listConnectionIDs()[0]);
      }

      assertEquals(0, getProtocolManager().getStateManager().getQos2PacketIdCorrelationSize(SUBSCRIBER_CLIENT_ID));
      assertTrue(getSubCache(SUBSCRIBER_CLIENT_ID).contains(ByteUtil.intToBytes(1)));

      MQTTInterceptor pubCompInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(pubCompInterceptor);

      subscriber.reconnect();

      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));

      Wait.assertEquals(0L, () -> getSubscriptionQueue(TOPIC, SUBSCRIBER_CLIENT_ID).getMessageCount(), 500, 25);

      assertEquals(0, getProtocolManager().getStateManager().getQos2PacketIdCorrelationSize(SUBSCRIBER_CLIENT_ID));
      assertEquals(0, getSubCacheSize(SUBSCRIBER_CLIENT_ID));

      subscriber.disconnect();

      // connect again to clean the session which will completely remove the cache from memory and disk
      subscriber.connect(new MqttConnectionOptionsBuilder().cleanStart(true).sessionExpiryInterval(0L).build());
      assertNull(getSubCache(SUBSCRIBER_CLIENT_ID));
      subscriber.disconnect();
      subscriber.close();
   }

   /**
    * Verifies that the broker correctly completes the QoS 2 flow if it sends the PUBREL but fails to receive the
    * corresponding PUBCOMP before the broker restarts.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartAfterPubRelSent() throws Exception {
      final String TOPIC = "test/resiliency";
      final String SUBSCRIBER_CLIENT_ID = "subscriber";
      final String PUBLISHER_CLIENT_ID = "publisher";
      final CountDownLatch pubCompBlockedLatch = new CountDownLatch(1);
      final CountDownLatch stopLatch = new CountDownLatch(1);
      final CountDownLatch pubCompLatch = new CountDownLatch(1);

      // Block the incoming PUBCOMP so the broker has sent PUBREL but never processes the PUBCOMP
      MQTTInterceptor pubCompBlocker = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompBlockedLatch.countDown();
            try {
               stopLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
            return false;
         }
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(pubCompBlocker);

      // Consumer with persistent session
      MqttClient consumer = createPahoClient(SUBSCRIBER_CLIENT_ID);
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            logger.info("messageArrived({}, {})", topic, message);
         }
      });
      MqttConnectionOptions subscriberOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(subscriberOptions);
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Producer
      MqttClient producer = createPahoClient(PUBLISHER_CLIENT_ID);
      producer.connect();

      producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);

      producer.disconnect();
      producer.close();

      assertTrue(pubCompBlockedLatch.await(5, TimeUnit.SECONDS));
      stopLatch.countDown();
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      MQTTInterceptor pubCompInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(pubCompInterceptor);

      consumer.reconnect();

      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));

      Wait.assertEquals(0L, () -> getSubscriptionQueue(TOPIC, SUBSCRIBER_CLIENT_ID).getMessageCount(), 500, 25);

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Verifies that after a complete QoS 2 protocol exchange (PUBLISH, PUBREC, PUBREL, PUBCOMP), a broker restart does
    * not cause re-delivery.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartAfterPubCompSent() throws Exception {
      final String TOPIC = "test/resiliency";
      final String SUBSCRIBER_CLIENT_ID = "subscriber";
      final String PUBLISHER_CLIENT_ID = "publisher";
      final CountDownLatch pubCompLatch = new CountDownLatch(2);
      AtomicInteger messageCount = new AtomicInteger(0);

      // Consumer with persistent session
      MqttClient consumer = createPahoClient(SUBSCRIBER_CLIENT_ID);
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            messageCount.incrementAndGet();
            logger.info("messageArrived({}, {})", topic, message);
         }
      });
      MqttConnectionOptions subscriberOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(subscriberOptions);
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Track PUBCOMPs to know when both QoS 2 flows are complete
      MQTTInterceptor pubCompInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(pubCompInterceptor);

      // Producer
      MqttClient producer = createPahoClient(PUBLISHER_CLIENT_ID);
      producer.connect();

      producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);
      producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);

      producer.disconnect();
      producer.close();

      // Wait for both QoS 2 flows to complete
      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));
      Wait.assertEquals(0L, () -> getSubscriptionQueue(TOPIC, SUBSCRIBER_CLIENT_ID).getMessageCount(), 500, 25);

      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // Track any unexpected outgoing PUBLISH after restart
      CountDownLatch unexpectedPublishLatch = new CountDownLatch(1);
      MQTTInterceptor publishInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            unexpectedPublishLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(publishInterceptor);

      int countBeforeReconnect = messageCount.get();
      consumer.reconnect();

      // Give time for any unexpected re-delivery, then verify none occurred
      Thread.sleep(500);
      assertTrue(unexpectedPublishLatch.getCount() > 0, "Unexpected PUBLISH sent after restart");
      assertTrue(messageCount.get() == countBeforeReconnect, "Unexpected message delivered after restart");
      Wait.assertEquals(0L, () -> getSubscriptionQueue(TOPIC, SUBSCRIBER_CLIENT_ID).getMessageCount(), 500, 25);

      consumer.disconnect();
      consumer.close();
   }
}
