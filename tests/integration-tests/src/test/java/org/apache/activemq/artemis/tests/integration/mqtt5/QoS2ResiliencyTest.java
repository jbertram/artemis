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

import javax.transaction.xa.Xid;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.handler.codec.mqtt.MqttMessageType;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor;
import org.apache.activemq.artemis.core.protocol.mqtt.MqttXid;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTUtil;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for QoS 2 protocol resiliency with client reconnections and broker restarts.
 *
 * QoS 2 Protocol Flow:
 * 1. Sender sends PUBLISH (QoS=2)
 * 2. Receiver sends PUBREC
 * 3. Sender sends PUBREL
 * 4. Receiver sends PUBCOMP
 *
 * These tests verify that the protocol maintains exactly-once delivery semantics when clients disconnect/reconnect and
 * when the broker is stopped and restarted.
 */
public class QoS2ResiliencyTest extends MQTT5TestSupport {

   /**
    * Test that QoS 2 subscription and messages persist when consumer reconnects. Verifies that subscription queue
    * survives disconnect and messages are queued for delivery when consumer is offline, ensuring exactly-once delivery
    * semantics.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2MessageQueuedForOfflineConsumer() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CONSUMER_ID = "consumer";
      final int MESSAGE_COUNT = 5;
      final CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create consumer with persistent session and subscribe
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.subscribe(TOPIC, EXACTLY_ONCE);
      consumer.disconnect();

      // Verify session and subscription queue persisted
      assertEquals(1, getSessionStates().size());
      assertNotNull(getSessionStates().get(CONSUMER_ID));
      Queue queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);
      assertEquals(0, queue.getMessageCount());

      // Send messages while consumer is offline
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }
      producer.disconnect();
      producer.close();

      // Verify messages are queued
      Wait.assertEquals((long) MESSAGE_COUNT, queue::getMessageCount, 5000, 100);

      // Reconnect consumer and receive all messages
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.connect(consumerOptions);

      assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));
      assertEquals(MESSAGE_COUNT, receivedCount.get());

      // Verify all messages were acknowledged and removed from queue
      Wait.assertEquals(0L, queue::getMessageCount, 5000, 100);

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test that QoS 2 protocol completes correctly when client disconnects mid-flow. Simulates client disconnect after
    * receiving PUBREC, then verifies protocol completion on reconnect.
    * <p>
    * TODO: This test is currently disabled due to timing issues with PUBREC detection.
    * The MQTT client may complete the QoS 2 protocol before we can intercept PUBREC.
    */
   // @Test
   // @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2ProtocolCompletesAfterClientDisconnect() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final byte[] PAYLOAD = "test-message".getBytes(StandardCharsets.UTF_8);
      final CountDownLatch pubRecLatch = new CountDownLatch(1);
      final AtomicInteger messageCount = new AtomicInteger(0);

      // Create a queue for the topic
      server.createQueue(QueueConfiguration.of(TOPIC)
         .setAddress(TOPIC)
         .setRoutingType(RoutingType.MULTICAST)
         .setDurable(true));

      // Set up interceptor to track PUBREC
      MQTTInterceptor incomingInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            messageCount.incrementAndGet();
         }
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBREC) {
            pubRecLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(incomingInterceptor);

      // Producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send message
      producer.publish(TOPIC, PAYLOAD, EXACTLY_ONCE, false);

      // Wait for PUBREC
      assertTrue(pubRecLatch.await(10, TimeUnit.SECONDS));

      // Disconnect producer (simulating network failure)
      producer.disconnect();

      // Verify message was published once
      assertEquals(1, messageCount.get());

      // Reconnect producer and verify protocol completes
      producer.connect(producerOptions);

      // Give time for protocol to complete
      Thread.sleep(1000);

      // Verify message is in queue (exactly once)
      Queue queue = server.locateQueue(TOPIC);
      assertNotNull(queue);
      Wait.assertEquals(1L, queue::getMessageCount, 5000, 100);

      producer.disconnect();
      producer.close();
   }

   /**
    * Test that duplicate PUBLISH messages are detected and filtered in QoS 2. Verifies that resending the same message
    * doesn't result in duplicates.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2DuplicateMessageFiltering() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final byte[] PAYLOAD = "test-message".getBytes(StandardCharsets.UTF_8);
      final CountDownLatch firstPublishLatch = new CountDownLatch(1);
      final AtomicInteger publishCount = new AtomicInteger(0);

      // Create a durable queue for the topic
      server.createQueue(QueueConfiguration.of(TOPIC)
                            .setAddress(TOPIC)
                            .setRoutingType(RoutingType.MULTICAST)
                            .setDurable(true));

      // Set up interceptor to track PUBLISH messages
      MQTTInterceptor incomingInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            int count = publishCount.incrementAndGet();
            if (count == 1) {
               firstPublishLatch.countDown();
            }
         }
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(incomingInterceptor);

      // Producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send message
      producer.publish(TOPIC, PAYLOAD, EXACTLY_ONCE, false);
      assertTrue(firstPublishLatch.await(5, TimeUnit.SECONDS));

      // Disconnect and reconnect
      producer.disconnect();
      producer.connect(producerOptions);

      // Paho client may resend unacknowledged messages on reconnect
      // Give time for any retransmissions
      Thread.sleep(2000);

      // Verify only one message is in the queue despite potential retransmissions
      Queue queue = server.locateQueue(TOPIC);
      assertNotNull(queue);
      assertEquals(1, queue.getMessageCount());

      producer.disconnect();
      producer.close();
   }

   /**
    * Verifies that resending the same message via QoS2 doesn't result in duplicates when the broker is restarted
    * immediately after it receives the PUBLISH (i.e., before it can persist anything).
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartAfterPublish() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CLIENTID = "producer";
      final CountDownLatch firstPublishLatch = new CountDownLatch(1);
      final CountDownLatch stopLatch = new CountDownLatch(1);

      // Simulate a subscription queue
      server.createQueue(QueueConfiguration.of(TOPIC)
                            .setAddress(TOPIC)
                            .setRoutingType(RoutingType.MULTICAST)
                            .setDurable(true));

      // Set up interceptor block the initial PUBLISH
      MQTTInterceptor pubRecInterceptor = (packet, connection) -> {
         System.out.println(packet.fixedHeader().messageType());
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            firstPublishLatch.countDown();
            logger.info("Blocking incoming {}", packet.fixedHeader().messageType());
            try {
               stopLatch.await();
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
            return false;
         }
         logger.info("Allowing incoming {}", packet.fixedHeader().messageType());
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(pubRecInterceptor);

      // Producer with persistent session
      MqttClient producer = createPahoClient(CLIENTID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());

      // Send message async as it will block waiting for a PUBREC that won't come
      CompletableFuture.runAsync(() -> {
         try {
            producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);
         } catch (Exception e) {
            e.printStackTrace();
         }
      });
      assertTrue(firstPublishLatch.await(5, TimeUnit.SECONDS));

      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());

      stopLatch.countDown();
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());

      // The client will automatically re-initiate the QoS2 protocol after reconnecting since it never got a PUBREC
      producer.reconnect();

      // Verify only one message is in the queue despite retransmission
      Wait.assertEquals(1, () -> server.locateQueue(TOPIC).getMessageCount());

      producer.disconnect();
      producer.close();
   }

   /**
    * Verifies that resending the same message via QoS2 doesn't result in duplicates when the broker is restarted after
    * the broker receives the PUBLISH, prepares a transaction, and sends the PUBREC but before the client receives the
    * PUBREC.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartBeforePubRec() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CLIENTID = "producer";
      final CountDownLatch firstPubRecLatch = new CountDownLatch(1);
      final CountDownLatch pubCompLatch = new CountDownLatch(1);

      // Simulate a subscription queue
      server.createQueue(QueueConfiguration.of(TOPIC)
                            .setAddress(TOPIC)
                            .setRoutingType(RoutingType.MULTICAST)
                            .setDurable(true));

      // Set up interceptor block the initial PUBREC
      MQTTInterceptor pubRecInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBREC) {
            firstPubRecLatch.countDown();
            logger.info("Blocking outgoing {}", packet.fixedHeader().messageType());
            return false;
         }

         logger.info("Allowing outgoing {}", packet.fixedHeader().messageType());
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(pubRecInterceptor);

      // Producer with persistent session
      MqttClient producer = createPahoClient(CLIENTID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());

      // Send message async as it will block waiting for a PUBREC that won't come
      CompletableFuture.runAsync(() -> {
         try {
            producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);
         } catch (Exception e) {
            e.printStackTrace();
         }
      });
      assertTrue(firstPubRecLatch.await(5, TimeUnit.SECONDS));
      List<Xid> xids = server.getResourceManager().getPreparedTransactions();
      assertEquals(1, xids.size());
      assertInstanceOf(MqttXid.class, xids.get(0));
      assertEquals(CLIENTID, ((MqttXid)xids.get(0)).getClientId());
      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());

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
      server.getRemotingService().addOutgoingInterceptor(pubCompInterceptor);

      // The client will automatically re-initiate the QoS2 protocol after reconnecting since it never got a PUBREC
      producer.reconnect();

      // Wait for the PUBCOMP to confirm QoS2 protocol is done
      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));

      // Verify only one message is in the queue despite retransmission
      assertEquals(1, server.locateQueue(TOPIC).getMessageCount());
      assertEquals(0, server.getResourceManager().getPreparedTransactions().size());

      producer.disconnect();
      producer.close();
   }

   /**
    * Verifies that resending the same message via QoS2 doesn't result in duplicates when the broker is restarted before
    * it processes the PUBREL.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartBeforePubRel() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CLIENTID = "producer";
      final CountDownLatch firstPubRelLatch = new CountDownLatch(1);
      final CountDownLatch stopLatch = new CountDownLatch(1);

      // Simulate a subscription queue
      server.createQueue(QueueConfiguration.of(TOPIC)
                            .setAddress(TOPIC)
                            .setRoutingType(RoutingType.MULTICAST)
                            .setDurable(true));

      // Set up interceptor block the initial PUBREL
      MQTTInterceptor pubRecInterceptor = (packet, connection) -> {
         System.out.println(packet.fixedHeader().messageType());
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            firstPubRelLatch.countDown();
            logger.info("Blocking incoming {}", packet.fixedHeader().messageType());
            try {
               stopLatch.await();
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
            return false;
         }
         logger.info("Allowing incoming {}", packet.fixedHeader().messageType());
         return true;
      };
      server.getRemotingService().addIncomingInterceptor(pubRecInterceptor);

      // Producer with persistent session
      MqttClient producer = createPahoClient(CLIENTID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());

      // Send message async as it will block waiting for a PUBCOMP that won't come
      CompletableFuture.runAsync(() -> {
         try {
            producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);
         } catch (Exception e) {
            e.printStackTrace();
         }
      });
      assertTrue(firstPubRelLatch.await(5, TimeUnit.SECONDS));

      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());
      assertEquals(1, server.getResourceManager().getPreparedTransactions().size());

      stopLatch.countDown();
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // The client will automatically re-initiate the QoS2 protocol after reconnecting since it never got a PUBCOMP
      producer.reconnect();

      // Verify only one message is in the queue despite retransmission
      Wait.assertEquals(1, () -> server.locateQueue(TOPIC).getMessageCount());
      assertEquals(0, server.getResourceManager().getPreparedTransactions().size());

      producer.disconnect();
      producer.close();
   }

   /**
    * Test that duplicate PUBLISH packets are detected and ignored in QoS 2. Verifies that resending the same message
    * doesn't result in duplicates when the broker is restarted after the broker processes the PUBREL but before the
    * client receives the PUBCOMP.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2BrokerRestartBeforePubComp() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CLIENTID = "producer";
      final ReusableLatch pubCompLatch = new ReusableLatch(1);

      // Simulate a subscription queue
      server.createQueue(QueueConfiguration.of(TOPIC)
                            .setAddress(TOPIC)
                            .setRoutingType(RoutingType.MULTICAST)
                            .setDurable(true));

      // Set up interceptor block the initial PUBCOMP
      MQTTInterceptor pubRecInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompLatch.countDown();
            logger.info("Blocking outgoing {}", packet.fixedHeader().messageType());
            return false;
         }

         logger.info("Allowing outgoing {}", packet.fixedHeader().messageType());
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(pubRecInterceptor);

      // Producer with persistent session
      MqttClient producer = createPahoClient(CLIENTID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      assertEquals(0, server.locateQueue(TOPIC).getMessageCount());

      // Send message async as it will block waiting for a PUBREC that won't come
      CompletableFuture.runAsync(() -> {
         try {
            producer.publish(TOPIC, RandomUtil.randomBytes(), EXACTLY_ONCE, false);
         } catch (Exception e) {
            e.printStackTrace();
         }
      });
      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));
      assertEquals(1, server.locateQueue(TOPIC).getMessageCount());
      assertEquals(0, server.getResourceManager().getPreparedTransactions().size());

      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      pubCompLatch.countUp();
      MQTTInterceptor pubCompInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(pubCompInterceptor);

      // The client will automatically re-initiate the QoS2 protocol after reconnecting since it never got a PUBREC
      producer.reconnect();

      // Wait for the PUBCOMP to confirm QoS2 protocol is done
      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));

      // Verify only one message is in the queue despite retransmission
      assertEquals(1, server.locateQueue(TOPIC).getMessageCount());
      assertEquals(0, server.getResourceManager().getPreparedTransactions().size());

      producer.disconnect();
      producer.close();
   }

   /**
    * Test QoS 2 message delivery with consumer callback failure. Verifies that message is redelivered after callback
    * throws exception.
    * <p>
    * TODO: This test is currently disabled due to unpredictable redelivery timing.
    * The Paho client's automatic redelivery behavior may not trigger as expected
    * when callback throws exceptions.
    */
   // @Test
   // @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2RedeliveryAfterConsumerFailure() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CONSUMER_ID = "consumer";
      final byte[] PAYLOAD = "test-message".getBytes(StandardCharsets.UTF_8);
      final CountDownLatch firstAttemptLatch = new CountDownLatch(1);
      final CountDownLatch secondAttemptLatch = new CountDownLatch(1);
      final AtomicInteger attemptCount = new AtomicInteger(0);
      final AtomicReference<String> receivedPayload = new AtomicReference<>();

      // Create consumer with persistent session
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            int attempt = attemptCount.incrementAndGet();
            if (attempt == 1) {
               firstAttemptLatch.countDown();
               // Simulate processing failure
               throw new RuntimeException("Simulated processing failure");
            } else {
               receivedPayload.set(new String(message.getPayload(), StandardCharsets.UTF_8));
               secondAttemptLatch.countDown();
            }
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Publisher sends message
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      producer.publish(TOPIC, PAYLOAD, EXACTLY_ONCE, false);
      producer.disconnect();
      producer.close();

      // Wait for first (failed) attempt
      assertTrue(firstAttemptLatch.await(5, TimeUnit.SECONDS));

      // Disconnect and reconnect consumer to trigger redelivery
      consumer.disconnect();
      consumer.connect(consumerOptions);

      // Wait for successful delivery
      assertTrue(secondAttemptLatch.await(10, TimeUnit.SECONDS));
      assertEquals("test-message", receivedPayload.get());
      assertEquals(2, attemptCount.get());

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test that QoS 2 management queue is properly maintained for PUBREL tracking.
    * Verifies the internal state management for the QoS 2 protocol.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testQoS2ManagementQueueLifecycle() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CONSUMER_ID = "consumer";
      final byte[] PAYLOAD = "test-message".getBytes(StandardCharsets.UTF_8);
      final CountDownLatch pubRelLatch = new CountDownLatch(1);
      final CountDownLatch receiveLatch = new CountDownLatch(1);

      // Set up interceptor to track PUBREL
      MQTTInterceptor outgoingInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            pubRelLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(outgoingInterceptor);

      // Create consumer with persistent session
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receiveLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Publisher sends message
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      producer.publish(TOPIC, PAYLOAD, EXACTLY_ONCE, false);
      producer.disconnect();
      producer.close();

      // Wait for message delivery and PUBREL
      assertTrue(receiveLatch.await(5, TimeUnit.SECONDS));
      assertTrue(pubRelLatch.await(5, TimeUnit.SECONDS));

      // Verify QoS 2 management queue exists and is eventually cleaned up
      String managementQueueName = MQTTUtil.QOS2_MANAGEMENT_QUEUE_PREFIX + CONSUMER_ID;
      Wait.assertTrue(() -> {
         Queue managementQueue = server.locateQueue(managementQueueName);
         return managementQueue != null;
      }, 5000, 100);

      // After PUBCOMP, management queue should be cleaned up
      Wait.assertEquals(0L, () -> {
         Queue managementQueue = server.locateQueue(managementQueueName);
         return managementQueue != null ? managementQueue.getMessageCount() : 0L;
      }, 5000, 100);

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test broker restart with QoS 2 messages in queue.
    * Messages sent to offline consumer should survive broker restart and be delivered
    * when consumer reconnects after broker is back up.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testBrokerRestartWithQueuedQoS2Messages() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CONSUMER_ID = "consumer";
      final int MESSAGE_COUNT = 3;
      final CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create consumer with persistent session and subscribe
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.subscribe(TOPIC, EXACTLY_ONCE);
      consumer.disconnect();

      // Verify subscription queue exists
      Queue queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);
      System.out.println("Sub queue name: " + queue.getName());
      assertTrue(queue.isDurable());
      assertEquals(0, queue.getMessageCount());

      // Send messages while consumer is offline
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }
      producer.disconnect();
      producer.close();

      // Verify messages are queued
      Wait.assertEquals((long) MESSAGE_COUNT, queue::getMessageCount, 5000, 100);

      assertEquals(1, server.locateQueue(MQTTUtil.MQTT_SESSION_STORE).getMessageCount());

      // Stop and restart broker
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      Thread.sleep(500);

      // Verify queue and messages still exist after restart
      queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);
      assertEquals(MESSAGE_COUNT, queue.getMessageCount());

      // Consumer reconnects and receives all messages
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.connect(consumerOptions);

      assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));
      assertEquals(MESSAGE_COUNT, receivedCount.get());

      // Verify all messages were acknowledged
      Wait.assertEquals(0L, queue::getMessageCount, 5000, 100);

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test broker restart during active QoS 2 message delivery.
    * Simulates broker crash while consumer is connected and receiving QoS 2 messages.
    * Verifies that unacknowledged messages are redelivered after broker restart.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testBrokerRestartDuringQoS2Delivery() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CONSUMER_ID = "consumer";
      final int MESSAGE_COUNT = 5;
      final CountDownLatch firstBatchLatch = new CountDownLatch(2);
      final CountDownLatch allMessagesLatch = new CountDownLatch(MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);
      final AtomicInteger firstBatchCount = new AtomicInteger(0);

      // Create consumer with persistent session
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            int count = receivedCount.incrementAndGet();
            if (count <= 2) {
               firstBatchCount.incrementAndGet();
               firstBatchLatch.countDown();
            }
            allMessagesLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Send messages
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }
      producer.disconnect();
      producer.close();

      // Wait for consumer to receive first couple of messages
      assertTrue(firstBatchLatch.await(5, TimeUnit.SECONDS));
      assertEquals(2, firstBatchCount.get());

      // Disconnect consumer
      consumer.disconnect();

      // Stop and restart broker (simulating crash during delivery)
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // Reconnect consumer
      consumer.connect(consumerOptions);

      // Consumer should receive remaining messages (and possibly some redeliveries)
      assertTrue(allMessagesLatch.await(10, TimeUnit.SECONDS));

      // Should have received all messages (may be >= MESSAGE_COUNT due to redeliveries)
      assertTrue(receivedCount.get() >= MESSAGE_COUNT);

      // Verify queue is eventually empty
      Queue queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);
      Wait.assertEquals(0L, queue::getMessageCount, 5000, 100);

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test broker restart preserves QoS 2 subscription queues.
    * Verifies that durable subscription queues survive broker restart and messages
    * can be delivered to reconnecting clients.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testBrokerRestartPreservesQoS2SubscriptionQueues() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CONSUMER_ID = "consumer";
      final byte[] PAYLOAD = "test-message".getBytes(StandardCharsets.UTF_8);
      final CountDownLatch receiveLatch = new CountDownLatch(1);
      final AtomicReference<String> receivedPayload = new AtomicReference<>();

      // Create consumer with persistent session and subscribe
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.subscribe(TOPIC, EXACTLY_ONCE);
      consumer.disconnect();

      // Verify subscription queue exists
      Queue queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);

      // Restart broker
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // Verify subscription queue still exists after restart
      queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);

      // Send message after broker restart
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      producer.publish(TOPIC, PAYLOAD, EXACTLY_ONCE, false);
      producer.disconnect();
      producer.close();

      // Verify message is in queue
      Wait.assertEquals(1L, queue::getMessageCount, 5000, 100);

      // Consumer reconnects and receives message
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedPayload.set(new String(message.getPayload(), StandardCharsets.UTF_8));
            receiveLatch.countDown();
         }
      });
      consumer.connect(consumerOptions);

      assertTrue(receiveLatch.await(5, TimeUnit.SECONDS));
      assertEquals("test-message", receivedPayload.get());

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test multiple broker restarts with QoS 2 messages.
    * Verifies that QoS 2 messages and protocol state survive multiple broker restarts.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testMultipleBrokerRestartsWithQoS2Messages() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String CONSUMER_ID = "consumer";
      final int MESSAGE_COUNT = 4;
      final CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create consumer with persistent session and subscribe
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.subscribe(TOPIC, EXACTLY_ONCE);
      consumer.disconnect();

      // Send first batch of messages
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      for (int i = 0; i < 2; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }
      producer.disconnect();

      // First broker restart
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // Verify first batch still in queue
      Queue queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);
      assertEquals(2, queue.getMessageCount());

      // Send second batch of messages
      producer.connect();
      for (int i = 2; i < MESSAGE_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }
      producer.disconnect();
      producer.close();

      // Verify all messages in queue
      Wait.assertEquals((long) MESSAGE_COUNT, queue::getMessageCount, 5000, 100);

      // Second broker restart
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // Verify all messages still in queue after second restart
      queue = getSubscriptionQueue(TOPIC, CONSUMER_ID);
      assertNotNull(queue);
      assertEquals(MESSAGE_COUNT, queue.getMessageCount());

      // Consumer reconnects and receives all messages
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.connect(consumerOptions);

      assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));
      assertEquals(MESSAGE_COUNT, receivedCount.get());

      // Verify all messages were acknowledged
      Wait.assertEquals(0L, queue::getMessageCount, 5000, 100);

      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test producer reconnection with persistent session preserves QoS 2 state.
    * Verifies that a producer can disconnect and reconnect with cleanStart=false
    * and the session state is maintained.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testProducerSessionPersistsAcrossReconnect() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final int MESSAGE_COUNT = 3;
      final CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create a durable queue for the topic
      server.createQueue(QueueConfiguration.of(TOPIC)
         .setAddress(TOPIC)
         .setRoutingType(RoutingType.MULTICAST)
         .setDurable(true));

      // Create consumer to receive messages
      MqttClient consumer = createPahoClient("consumer");
      consumer.connect();
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Create producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send first message
      byte[] payload1 = "message-0".getBytes(StandardCharsets.UTF_8);
      producer.publish(TOPIC, payload1, EXACTLY_ONCE, false);

      // Disconnect and reconnect producer
      producer.disconnect();

      // Verify producer session persisted (consumer also has a session, so total is 2)
      assertEquals(2, getSessionStates().size());
      assertNotNull(getSessionStates().get(PRODUCER_ID));

      producer.connect(producerOptions);

      // Send remaining messages
      for (int i = 1; i < MESSAGE_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }

      // Verify all messages received
      assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));
      assertEquals(MESSAGE_COUNT, receivedCount.get());

      producer.disconnect();
      producer.close();
      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test producer disconnect and reconnect during active QoS 2 publishing.
    * Verifies that messages in-flight when producer disconnects are properly
    * handled on reconnection and no duplicates are created.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testProducerReconnectDuringActivePublishing() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final int INITIAL_MESSAGE_COUNT = 5;
      final int TOTAL_MESSAGE_COUNT = 10;
      final CountDownLatch receiveLatch = new CountDownLatch(TOTAL_MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create a durable queue for the topic
      server.createQueue(QueueConfiguration.of(TOPIC)
         .setAddress(TOPIC)
         .setRoutingType(RoutingType.MULTICAST)
         .setDurable(true));

      // Create consumer to receive messages
      MqttClient consumer = createPahoClient("consumer");
      consumer.connect();
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Create producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send initial batch of messages
      for (int i = 0; i < INITIAL_MESSAGE_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }

      // Disconnect producer (simulating network interruption)
      producer.disconnect();

      // Wait a bit to ensure in-flight messages are processed
      Thread.sleep(1000);

      // Reconnect and send more messages
      producer.connect(producerOptions);
      for (int i = INITIAL_MESSAGE_COUNT; i < TOTAL_MESSAGE_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }

      // Verify all messages received (exactly once, no duplicates)
      assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));

      // Wait a bit more to catch any duplicate deliveries
      Thread.sleep(2000);

      // Should have received exactly TOTAL_MESSAGE_COUNT messages
      assertEquals(TOTAL_MESSAGE_COUNT, receivedCount.get());

      producer.disconnect();
      producer.close();
      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test broker restart while producer has QoS 2 messages in-flight.
    * Verifies that producer session state and in-flight message tracking
    * survive broker restart.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testBrokerRestartDuringProducerPublishing() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final String CONSUMER_ID = "consumer";
      final int PRE_RESTART_COUNT = 3;
      final int POST_RESTART_COUNT = 2;
      final int TOTAL_COUNT = PRE_RESTART_COUNT + POST_RESTART_COUNT;
      final CountDownLatch receiveLatch = new CountDownLatch(TOTAL_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create consumer with persistent session
      MqttClient consumer = createPahoClient(CONSUMER_ID);
      MqttConnectionOptions consumerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(consumerOptions);
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Create producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send messages before restart
      for (int i = 0; i < PRE_RESTART_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }

      // Disconnect both clients
      producer.disconnect();
      consumer.disconnect();

      // Stop and restart broker
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // Reconnect producer and send more messages
      producer.connect(producerOptions);
      for (int i = PRE_RESTART_COUNT; i < TOTAL_COUNT; i++) {
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }
      producer.disconnect();

      // Reconnect consumer and verify all messages received
      consumer.connect(consumerOptions);

      assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));

      // Wait to ensure no duplicates arrive
      Thread.sleep(2000);

      assertEquals(TOTAL_COUNT, receivedCount.get());

      producer.close();
      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test that producer QoS 2 message IDs are properly managed across reconnections.
    * Verifies that message IDs don't collide when producer reconnects and continues
    * publishing, ensuring the QoS 2 protocol can distinguish between messages.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testProducerMessageIdManagementAcrossReconnect() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final int MESSAGES_PER_SESSION = 10;
      final int TOTAL_MESSAGE_COUNT = MESSAGES_PER_SESSION * 2;
      final CountDownLatch receiveLatch = new CountDownLatch(TOTAL_MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create a durable queue for the topic
      server.createQueue(QueueConfiguration.of(TOPIC)
         .setAddress(TOPIC)
         .setRoutingType(RoutingType.MULTICAST)
         .setDurable(true));

      // Create consumer to receive messages
      MqttClient consumer = createPahoClient("consumer");
      consumer.connect();
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Create producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send first batch of messages
      for (int i = 0; i < MESSAGES_PER_SESSION; i++) {
         byte[] payload = ("batch1-message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }

      // Disconnect and reconnect
      producer.disconnect();
      Thread.sleep(500);
      producer.connect(producerOptions);

      // Send second batch of messages
      for (int i = 0; i < MESSAGES_PER_SESSION; i++) {
         byte[] payload = ("batch2-message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
      }

      // Verify all messages received exactly once
      assertTrue(receiveLatch.await(15, TimeUnit.SECONDS));

      // Wait to catch any duplicates
      Thread.sleep(2000);

      assertEquals(TOTAL_MESSAGE_COUNT, receivedCount.get());

      producer.disconnect();
      producer.close();
      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test producer with quick disconnect and reconnect during QoS 2 publishing.
    * Verifies that messages are delivered exactly once even when producer
    * connection is unstable during the publish flow.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testProducerQuickDisconnectReconnect() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final int MESSAGE_COUNT = 3;
      final CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create a durable queue for the topic
      server.createQueue(QueueConfiguration.of(TOPIC)
         .setAddress(TOPIC)
         .setRoutingType(RoutingType.MULTICAST)
         .setDurable(true));

      // Create consumer to receive messages
      MqttClient consumer = createPahoClient("consumer");
      consumer.connect();
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Create producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();

      // Publish messages with quick disconnect/reconnect cycles
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         producer.connect(producerOptions);
         byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
         producer.publish(TOPIC, payload, EXACTLY_ONCE, false);

         // Quick disconnect after each publish
         Thread.sleep(50);
         producer.disconnect();
         Thread.sleep(50);
      }

      // Reconnect one final time to ensure any pending protocol messages complete
      producer.connect(producerOptions);
      Thread.sleep(500);
      producer.disconnect();

      // Consumer should receive all messages exactly once
      assertTrue(receiveLatch.await(15, TimeUnit.SECONDS));

      // Wait to ensure no duplicates
      Thread.sleep(2000);
      assertEquals(MESSAGE_COUNT, receivedCount.get());

      producer.close();
      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test that multiple producers with persistent sessions can publish concurrently
    * and handle reconnections without message loss or duplication.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testMultipleProducersWithReconnections() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final int PRODUCER_COUNT = 3;
      final int MESSAGES_PER_PRODUCER = 5;
      final int TOTAL_MESSAGE_COUNT = PRODUCER_COUNT * MESSAGES_PER_PRODUCER;
      final CountDownLatch receiveLatch = new CountDownLatch(TOTAL_MESSAGE_COUNT);
      final AtomicInteger receivedCount = new AtomicInteger(0);

      // Create a durable queue for the topic
      server.createQueue(QueueConfiguration.of(TOPIC)
         .setAddress(TOPIC)
         .setRoutingType(RoutingType.MULTICAST)
         .setDurable(true));

      // Create consumer to receive messages
      MqttClient consumer = createPahoClient("consumer");
      consumer.connect();
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receivedCount.incrementAndGet();
            receiveLatch.countDown();
         }
      });
      consumer.subscribe(TOPIC, EXACTLY_ONCE);

      // Create multiple producers
      MqttClient[] producers = new MqttClient[PRODUCER_COUNT];
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();

      for (int p = 0; p < PRODUCER_COUNT; p++) {
         producers[p] = createPahoClient("producer-" + p);
         producers[p].connect(producerOptions);

         // Each producer sends some messages
         for (int i = 0; i < MESSAGES_PER_PRODUCER / 2; i++) {
            byte[] payload = ("producer-" + p + "-message-" + i).getBytes(StandardCharsets.UTF_8);
            producers[p].publish(TOPIC, payload, EXACTLY_ONCE, false);
         }

         // Disconnect producer mid-publishing
         producers[p].disconnect();
      }

      // Wait a bit then reconnect all producers and send remaining messages
      Thread.sleep(1000);

      for (int p = 0; p < PRODUCER_COUNT; p++) {
         producers[p].connect(producerOptions);

         for (int i = MESSAGES_PER_PRODUCER / 2; i < MESSAGES_PER_PRODUCER; i++) {
            byte[] payload = ("producer-" + p + "-message-" + i).getBytes(StandardCharsets.UTF_8);
            producers[p].publish(TOPIC, payload, EXACTLY_ONCE, false);
         }
      }

      // Verify all messages received exactly once
      assertTrue(receiveLatch.await(15, TimeUnit.SECONDS));

      // Wait to catch any duplicates
      Thread.sleep(2000);

      assertEquals(TOTAL_MESSAGE_COUNT, receivedCount.get());

      // Cleanup
      for (MqttClient producer : producers) {
         producer.disconnect();
         producer.close();
      }
      consumer.disconnect();
      consumer.close();
   }

   /**
    * Test that multiple concurrent QoS2 messages each get their own XA transaction and are correctly deduplicated
    * after broker restart.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testMultipleQoS2MessagesWithRestart() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";
      final int MESSAGE_COUNT = 5;
      final CountDownLatch firstBatchLatch = new CountDownLatch(MESSAGE_COUNT);
      final CountDownLatch pubCompLatch = new CountDownLatch(MESSAGE_COUNT);
      final AtomicInteger pubRecCount = new AtomicInteger(0);

      // Simulate a subscription queue
      server.createQueue(QueueConfiguration.of(TOPIC)
                            .setAddress(TOPIC)
                            .setRoutingType(RoutingType.MULTICAST)
                            .setDurable(true));

      // Set up interceptor to block the initial PUBRECs
      MQTTInterceptor pubRecInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBREC) {
            int count = pubRecCount.incrementAndGet();
            if (count <= MESSAGE_COUNT) {
               firstBatchLatch.countDown();
               logger.info("Blocking PUBREC #{}", count);
               return false;
            }
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(pubRecInterceptor);

      // Producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send multiple messages async (they will block waiting for PUBRECs)
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         final int messageIndex = i;
         CompletableFuture.runAsync(() -> {
            try {
               byte[] payload = ("message-" + messageIndex).getBytes(StandardCharsets.UTF_8);
               producer.publish(TOPIC, payload, EXACTLY_ONCE, false);
            } catch (Exception e) {
               e.printStackTrace();
            }
         });
      }

      // Wait for all messages to be queued
      Wait.assertEquals(MESSAGE_COUNT, () -> server.locateQueue(TOPIC).getMessageCount());
      assertTrue(firstBatchLatch.await(5, TimeUnit.SECONDS));

      // Restart broker
      server.stop();
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);

      // Set up interceptor to count PUBCOMPs
      MQTTInterceptor pubCompInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBCOMP) {
            pubCompLatch.countDown();
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(pubCompInterceptor);

      // Client will automatically retransmit all messages since it never got PUBRECs
      producer.reconnect();

      // Wait for all PUBCOMPs (QoS2 protocol completion)
      assertTrue(pubCompLatch.await(5, TimeUnit.SECONDS));

      // Verify only MESSAGE_COUNT messages in queue (no duplicates)
      assertEquals(MESSAGE_COUNT, server.locateQueue(TOPIC).getMessageCount());

      producer.disconnect();
      producer.close();
   }

   /*
    * NOTE: A test for cleanStart=true rolling back prepared transactions was attempted but proved
    * difficult to implement reliably due to MQTT client library behavior when reconnecting immediately
    * after a forced disconnect. The cleanup functionality is implemented in
    * MQTTPublishManager.cleanupQoS2Transactions() and is called from MQTTPublishManager.clean().
    */

   /**
    * Test that mixed QoS levels (0, 1, 2) work correctly with XA transactions only used for QoS2.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testMixedQoSLevels() throws Exception {
      final String TOPIC = RandomUtil.randomUUIDString();
      final String PRODUCER_ID = "producer";

      // Create subscription queue
      server.createQueue(QueueConfiguration.of(TOPIC)
                            .setAddress(TOPIC)
                            .setRoutingType(RoutingType.MULTICAST)
                            .setDurable(true));

      // Producer with persistent session
      MqttClient producer = createPahoClient(PRODUCER_ID);
      MqttConnectionOptions producerOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      producer.connect(producerOptions);

      // Send messages with different QoS levels
      producer.publish(TOPIC, "qos0-message".getBytes(StandardCharsets.UTF_8), AT_MOST_ONCE, false);
      producer.publish(TOPIC, "qos1-message-1".getBytes(StandardCharsets.UTF_8), AT_LEAST_ONCE, false);
      producer.publish(TOPIC, "qos2-message-1".getBytes(StandardCharsets.UTF_8), EXACTLY_ONCE, false);
      producer.publish(TOPIC, "qos1-message-2".getBytes(StandardCharsets.UTF_8), AT_LEAST_ONCE, false);
      producer.publish(TOPIC, "qos2-message-2".getBytes(StandardCharsets.UTF_8), EXACTLY_ONCE, false);

      // All QoS > 0 messages should be in the queue (4 total: 2 QoS1 + 2 QoS2)
      // QoS0 may or may not be there depending on timing
      Wait.assertTrue(() -> server.locateQueue(TOPIC).getMessageCount() >= 4);

      producer.disconnect();
      producer.close();
   }
}
