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
package org.apache.activemq.artemis.tests.soak.mqtt;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import io.reactivex.schedulers.Schedulers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTPacketIdCache;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTProtocolManager;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTUtil;
import org.apache.activemq.artemis.core.remoting.impl.AbstractAcceptor;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.TestParameters;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.cli.commands.tools.journal.CompactJournal.compactJournal;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager.ACTIVEMQ_DATA;
import static org.apache.activemq.artemis.core.protocol.mqtt.MQTTProtocolManagerFactory.MQTT_PROTOCOL_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QoS2ResiliencySoakTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String TEST_NAME = "QOS2_RESILIENCY_SOAK";
   private static final String TOPIC = "qos2/resiliency";
   private static final int MQTT_PORT = 1883;

   private static final int NUM_PUBLISHERS = TestParameters.testProperty(TEST_NAME, "NUM_PUBLISHERS", 20);
   private static final int NUM_SUBSCRIBERS = TestParameters.testProperty(TEST_NAME, "NUM_SUBSCRIBERS", 20);
   private static final int NUM_MESSAGES = TestParameters.testProperty(TEST_NAME, "NUM_MESSAGES", 50_000);
   private static final int RESTART_PAUSE = TestParameters.testProperty(TEST_NAME, "RESTART_PAUSE", 3_000);
   private static final int TIMEOUT_SECONDS = TestParameters.testProperty(TEST_NAME, "TIMEOUT_SECONDS", 1200);

   private ActiveMQServer server;

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();
      Schedulers.start();
      server = createServer(true, createDefaultConfig(true));
      server.getConfiguration().setJournalMinFiles(10).setJournalFileSize(25 * 1024 * 1024);
      server.getConfiguration().addAcceptorConfiguration(MQTT_PROTOCOL_NAME, "tcp://localhost:" + MQTT_PORT + "?protocols=MQTT");
      server.getConfiguration().setMqttSessionScanInterval(200);

      server.start();
      server.waitForActivation(10, TimeUnit.SECONDS);
   }

   private static void enableProtocolLogging() {
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
      LoggerConfig loggerConfig = new LoggerConfig(MQTTUtil.class.getName(), Level.TRACE, true);
      config.addLogger(MQTTUtil.class.getName(), loggerConfig);
      ctx.updateLoggers();
   }

   private static void disableProtocolLogging() {
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
      config.removeLogger(MQTTUtil.class.getName());
      ctx.updateLoggers();
   }

   @AfterEach
   @Override
   public void tearDown() throws Exception {
      if (server != null && server.isStarted()) {
         server.stop();
      }
      Schedulers.shutdown();
      super.tearDown();
   }

   @Test
   @Timeout(value = 30, unit = TimeUnit.MINUTES)
   public void testQoS2PublisherResiliency() throws Exception {
      disableProtocolLogging();
      final String PUB_CLIENT_ID_PREFIX = "pub-";
      final String SUB_CLIENT_ID = "sub";

      // create subscription queue for consuming messages later
      Mqtt5BlockingClient subscriber = createHiveMQClient(SUB_CLIENT_ID, false);
      subscriber.connectWith()
         .cleanStart(false)
         .sessionExpiryInterval(300)
         .send();
      subscriber.subscribeWith()
         .topicFilter(TOPIC)
         .qos(MqttQos.AT_LEAST_ONCE)
         .send();
      subscriber.disconnect();

      assertNotNull(getSubscriptionQueue(TOPIC, SUB_CLIENT_ID));

      logger.info("{} publishers, {} messages/publisher, {} total expected", NUM_PUBLISHERS, NUM_MESSAGES, NUM_PUBLISHERS * NUM_MESSAGES);

      // create and connect publishers
      final List<Mqtt5BlockingClient> publishers = new ArrayList<>();
      runAfter(() -> publishers.forEach(c -> {
         try {
            c.disconnect();
         } catch (Exception ignored) {
         }
      }));
      for (int i = 0; i < NUM_PUBLISHERS; i++) {
         String clientId = PUB_CLIENT_ID_PREFIX + i;
         Mqtt5BlockingClient publisher = createHiveMQClient(clientId, true);
         publisher.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(300)
            .send();
         publishers.add(publisher);
         logger.info("Publisher {} connected", clientId);
      }

      // Start broker restart task
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      ScheduledFuture restarter = scheduler.scheduleWithFixedDelay(() -> {
         try {
            logger.info("===========");
            logger.info("Subscription queue received {}/{} messages", getSubscriptionQueue(TOPIC, SUB_CLIENT_ID).getMessageCount(), NUM_PUBLISHERS * NUM_MESSAGES);
            logger.info("===========");

            logger.info("Stopping broker");
            server.stop();
            waitForServerToStop(server);

            // compacting keeps the journal small to reduce start-up time
            logger.info("Compacting journal...");
            compactJournal(server.getConfiguration().getJournalLocation(), server.getConfiguration().getJournalRetentionLocation(), ACTIVEMQ_DATA, "amq", server.getConfiguration().getJournalMinFiles(),
                           server.getConfiguration().getJournalPoolFiles(), server.getConfiguration().getJournalFileSize(), null, JournalRecordIds.UPDATE_DELIVERY_COUNT,
                           JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME);
            logger.info("Compacted journal.");

            server.start();
            waitForServerToStart(server);
         } catch (Exception e) {
            logger.warn("Error during broker restart", e);
         }
      }, RESTART_PAUSE, RESTART_PAUSE, TimeUnit.MILLISECONDS);

      // enableProtocolLogging();

      // Start publisher tasks
      final ExecutorService publisherExecutor = Executors.newFixedThreadPool(NUM_PUBLISHERS);
      runAfter(() -> {
         publisherExecutor.shutdownNow();
         try {
            publisherExecutor.awaitTermination(10, TimeUnit.SECONDS);
         } catch (InterruptedException ignored) {
         }
      });
      final Set<String> sentMessages = ConcurrentHashMap.newKeySet(NUM_PUBLISHERS * NUM_MESSAGES);
      final AtomicInteger publishErrors = new AtomicInteger(0);
      for (int i = 0; i < NUM_PUBLISHERS; i++) {
         final int pubId = i;
         final Mqtt5BlockingClient publisher = publishers.get(i);
         publisherExecutor.execute(() -> {
            for (int seq = 0; seq < NUM_MESSAGES; seq++) {
               String payload = pubId + "-" + seq;
               try {
                  waitForClientConnected(publisher);
                  Mqtt5PublishResult result = publisher.publishWith()
                     .topic(TOPIC)
                     .qos(MqttQos.EXACTLY_ONCE)
                     .payload(payload.getBytes(StandardCharsets.UTF_8))
                     .send();
                  if (result.getError().isPresent()) {
                     throw result.getError().get();
                  }
               } catch (Throwable e) {
                  publishErrors.incrementAndGet();
                  logger.info("Pub failed: {}; in-flight QoS 2 state will be resumed on reconnect", payload, e);
               }
               sentMessages.add(payload);
            }
            logger.info("Publisher {} finished sending all {} messages", pubId, NUM_MESSAGES);
         });
      }

      // wait for all publishers to finish
      publisherExecutor.shutdown();
      assertTrue(publisherExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Publishers did not finish in time");
      logger.info("All publishers finished. Total messages sent: {}. Publish errors: {}", sentMessages.size(), publishErrors.get());

      disableProtocolLogging();

      // stop restart task and ensure broker is running
      restarter.cancel(true);
      scheduler.shutdownNow();
      scheduler.awaitTermination(10, TimeUnit.SECONDS);
      if (!server.isStarted()) {
         server.start();
         waitForServerToStart(server);
      }

      final long messageCount = getSubscriptionQueue(TOPIC, SUB_CLIENT_ID).getMessageCount();

      // reconnect subscriber to verify there are no duplicates
      Set<String> consumedMessages = ConcurrentHashMap.newKeySet(NUM_MESSAGES * NUM_PUBLISHERS);
      AtomicInteger duplicateCount = new AtomicInteger(0);
      subscriber.toAsync().publishes(MqttGlobalPublishFilter.ALL, publish -> {
         String payload = new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);
         if (consumedMessages.contains(payload)) {
            logger.warn("Duplicate message: {}", payload);
            duplicateCount.incrementAndGet();
         }
         consumedMessages.add(payload);
         sentMessages.remove(payload);
      });

      subscriber.connectWith()
         .cleanStart(false)
         .sessionExpiryInterval(300)
         .send();

      Wait.waitFor(() -> consumedMessages.size() == messageCount);

      cleanDisconnect(subscriber);

      assertEquals(0, duplicateCount.get());
      assertEquals(0, sentMessages.size(), "These messages were published, but were not on the broker: " + sentMessages);
      assertEquals(NUM_PUBLISHERS * NUM_MESSAGES, consumedMessages.size());

      for (Mqtt5BlockingClient publisher : publishers) {
         String clientId = getClientId(publisher);
         Wait.assertEquals(0, () -> getPubCacheSize(clientId), 5000, 100);
         cleanDisconnect(publisher);
         assertNull(getPubCache(clientId), "Pub cache should be null after clean start for " + clientId);
      }
   }

   @Test
   @Timeout(value = 30, unit = TimeUnit.MINUTES)
   public void testQoS2SubscriberResiliency() throws Exception {
      disableProtocolLogging();
      logger.info("{} subscribers, {} messages/subscriber", NUM_SUBSCRIBERS, NUM_MESSAGES);

      // create and subscribe then disconnect to leave idle subscriptions on the broker
      final List<Mqtt5BlockingClient> subscribers = new ArrayList<>(NUM_SUBSCRIBERS);
      runAfter(() -> subscribers.forEach(c -> {
         try {
            c.disconnect();
         } catch (Exception ignored) {
         }
      }));
      for (int i = 0; i < NUM_SUBSCRIBERS; i++) {
         String clientId = "sub-" + i;
         Mqtt5BlockingClient subscriber = createHiveMQClient(clientId, true);
         subscriber.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(300)
            .send();
         subscriber.subscribeWith()
            .topicFilter(TOPIC)
            .qos(MqttQos.EXACTLY_ONCE)
            .send();
         subscriber.disconnect();
         subscribers.add(subscriber);
         assertNotNull(getSubscriptionQueue(TOPIC, getClientId(subscriber)));
      }

      // send messages using QoS 2
      final Set<String> sentMessages = new HashSet<>(NUM_MESSAGES);
      Mqtt5BlockingClient publisher = createHiveMQClient("pub", false);
      publisher.connectWith().cleanStart(true).send();
      logger.info("Publishing {} messages...", NUM_MESSAGES);
      for (int seq = 0; seq < NUM_MESSAGES; seq++) {
         String payload = String.valueOf(seq);
         sentMessages.add(payload);
         publisher.publishWith()
            .topic(TOPIC)
            .qos(MqttQos.EXACTLY_ONCE)
            .payload(payload.getBytes(StandardCharsets.UTF_8))
            .send();
      }
      logger.info("Published {} messages.", NUM_MESSAGES);
      cleanDisconnect(publisher);

      for (Mqtt5BlockingClient subscriber : subscribers) {
         assertEquals(NUM_MESSAGES, getSubscriptionQueue(TOPIC, getClientId(subscriber)).getMessageCount());
      }

      // enableProtocolLogging();

      final Map<String, Set<String>> receivedPerSubscriber = new HashMap<>();
      final Map<String, Set<String>> duplicatesPerSubscriber = new HashMap<>();
      final AtomicLong lastReceiveTime = new AtomicLong(System.currentTimeMillis());

      for (Mqtt5BlockingClient subscriber : subscribers) {
         final String clientId = getClientId(subscriber);
         final Set<String> received = ConcurrentHashMap.newKeySet(NUM_MESSAGES);
         receivedPerSubscriber.put(clientId, received);
         final Set<String> duplicates = ConcurrentHashMap.newKeySet();
         duplicatesPerSubscriber.put(clientId, duplicates);
         subscriber.toAsync().publishes(MqttGlobalPublishFilter.ALL, publish -> {
            String payload = new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);
            if (!received.add(payload)) {
               logger.warn("Subscriber {} received duplicate: {}", clientId, payload);
               duplicates.add(payload);
            }
            lastReceiveTime.set(System.currentTimeMillis());
         });
         subscriber.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(300)
            .send();
         logger.info("Subscriber {} reconnected", clientId);
      }

      // start broker restart task
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      ScheduledFuture restarter = scheduler.scheduleWithFixedDelay(() -> {
         try {
            logger.info("Stopping broker");
            server.stop();
            waitForServerToStop(server);

            logger.info("===========");
            for (Map.Entry<String, Set<String>> entry : receivedPerSubscriber.entrySet()) {
               logger.info("Subscriber {} received {}/{} messages", entry.getKey(), entry.getValue().size(), NUM_MESSAGES);
            }
            logger.info("Last message received {}ms ago.", System.currentTimeMillis() - lastReceiveTime.get());
            logger.info("===========");

            // compacting keeps the journal small to reduce start-up time
            logger.info("Compacting journal...");
            compactJournal(server.getConfiguration().getJournalLocation(), server.getConfiguration().getJournalRetentionLocation(), ACTIVEMQ_DATA, "amq", server.getConfiguration().getJournalMinFiles(),
                           server.getConfiguration().getJournalPoolFiles(), server.getConfiguration().getJournalFileSize(), null, JournalRecordIds.UPDATE_DELIVERY_COUNT,
                           JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME);
            logger.info("Compacted journal.");

            server.start();
            waitForServerToStart(server);
         } catch (Exception e) {
            logger.warn("Error during broker restart", e);
         }
      }, RESTART_PAUSE, RESTART_PAUSE, TimeUnit.MILLISECONDS);

      final long STALL_TIMEOUT_MS = 20_000;
      Wait.assertTrue(() -> {
         // quit early if subscribers are dead/stalled for some reason
         if (System.currentTimeMillis() - lastReceiveTime.get() > STALL_TIMEOUT_MS) {
            for (Map.Entry<String, Set<String>> entry : receivedPerSubscriber.entrySet()) {
               logger.warn("Subscriber {} received {}/{} messages", entry.getKey(), entry.getValue().size(), NUM_MESSAGES);
            }
            throw new AssertionError("No subscriber has received a message in " + STALL_TIMEOUT_MS / 1000 + " seconds");
         }
         // any duplicate is a failure, no need to wait until the end
         for (Map.Entry<String, Set<String>> duplicates : duplicatesPerSubscriber.entrySet()) {
            assertEquals(0, duplicates.getValue().size(), "Subscriber " + duplicates.getKey() + " received duplicates: " + duplicates.getValue());
         }
         for (Set<String> received : receivedPerSubscriber.values()) {
            if (received.size() < NUM_MESSAGES) {
               return false;
            }
         }
         return true;
      }, TIMEOUT_SECONDS * 1000L, 100);

      disableProtocolLogging();

      // stop reconnection task, ensure broker is running
      restarter.cancel(true);
      scheduler.shutdownNow();
      scheduler.awaitTermination(10, TimeUnit.SECONDS);
      if (!server.isStarted()) {
         server.start();
         waitForServerToStart(server);
      }

      // enableProtocolLogging();

      // verify all expected messages received with no duplicates
      for (Mqtt5BlockingClient subscriber : subscribers) {
         String clientId = getClientId(subscriber);
         assertEquals(0, duplicatesPerSubscriber.get(clientId).size(), "Subscriber " + getClientId(subscriber) + " received duplicates: " + duplicatesPerSubscriber.get(clientId));
         assertEquals(NUM_MESSAGES, receivedPerSubscriber.get(clientId).size(), "Subscriber " + getClientId(subscriber) + " didn't receive: " + getMissingMessages(sentMessages, receivedPerSubscriber.get(clientId)));
         assertEquals(0L, getSubscriptionQueue(TOPIC, getClientId(subscriber)).getMessageCount(), "Subscription queue for " + getClientId(subscriber) + " has incorrect message count");
         assertEquals(0, getProtocolManager().getStateManager().getPacketIdCorrelationSize(getClientId(subscriber)));
         assertEquals(0, getSubCacheSize(getClientId(subscriber)));
         cleanDisconnect(subscriber);
         assertNull(getSubCache(getClientId(subscriber)), "Sub cache should be null after clean start for " + getClientId(subscriber));
      }
   }

   @Test
   @Timeout(value = 30, unit = TimeUnit.MINUTES)
   public void testQoS2CombinedResiliency() throws Exception {
      disableProtocolLogging();
      final String PUB_CLIENT_ID_PREFIX = "pub-";
      final String SUB_CLIENT_ID_PREFIX = "sub-";

      logger.info("{} publishers, {} subscribers, {} messages/publisher, {} total expected per subscriber", NUM_PUBLISHERS, NUM_SUBSCRIBERS, NUM_MESSAGES, NUM_PUBLISHERS * NUM_MESSAGES);

      // create and subscribe all subscribers
      final List<Mqtt5BlockingClient> subscribers = new ArrayList<>(NUM_SUBSCRIBERS);
      runAfter(() -> subscribers.forEach(c -> {
         try {
            c.disconnect();
         } catch (Exception ignored) {
         }
      }));

      final Map<String, Set<String>> receivedPerSubscriber = new HashMap<>();
      final Map<String, Set<String>> duplicatesPerSubscriber = new HashMap<>();
      final AtomicLong lastReceiveTime = new AtomicLong(System.currentTimeMillis());

      for (int i = 0; i < NUM_SUBSCRIBERS; i++) {
         String clientId = SUB_CLIENT_ID_PREFIX + i;
         Mqtt5BlockingClient subscriber = createHiveMQClient(clientId, true);
         final Set<String> received = ConcurrentHashMap.newKeySet(NUM_PUBLISHERS * NUM_MESSAGES);
         receivedPerSubscriber.put(clientId, received);
         final Set<String> duplicates = ConcurrentHashMap.newKeySet();
         duplicatesPerSubscriber.put(clientId, duplicates);
         subscriber.toAsync().publishes(MqttGlobalPublishFilter.ALL, publish -> {
            String payload = new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);
            if (!received.add(payload)) {
               logger.warn("Subscriber {} received duplicate: {}", clientId, payload);
               duplicates.add(payload);
            }
            lastReceiveTime.set(System.currentTimeMillis());
         });
         subscriber.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(300)
            .send();
         subscriber.subscribeWith()
            .topicFilter(TOPIC)
            .qos(MqttQos.EXACTLY_ONCE)
            .send();
         subscribers.add(subscriber);
         assertNotNull(getSubscriptionQueue(TOPIC, clientId));
         logger.info("Subscriber {} connected and subscribed", clientId);
      }

      // create and connect publishers
      final List<Mqtt5BlockingClient> publishers = new ArrayList<>();
      runAfter(() -> publishers.forEach(c -> {
         try {
            c.disconnect();
         } catch (Exception ignored) {
         }
      }));
      for (int i = 0; i < NUM_PUBLISHERS; i++) {
         String clientId = PUB_CLIENT_ID_PREFIX + i;
         Mqtt5BlockingClient publisher = createHiveMQClient(clientId, true);
         publisher.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(300)
            .send();
         publishers.add(publisher);
         logger.info("Publisher {} connected", clientId);
      }

      // start publisher tasks
      final ExecutorService publisherExecutor = Executors.newFixedThreadPool(NUM_PUBLISHERS);
      runAfter(() -> {
         publisherExecutor.shutdownNow();
         try {
            publisherExecutor.awaitTermination(10, TimeUnit.SECONDS);
         } catch (InterruptedException ignored) {
         }
      });
      final Set<String> sentMessages = ConcurrentHashMap.newKeySet(NUM_PUBLISHERS * NUM_MESSAGES);
      final AtomicInteger publishErrors = new AtomicInteger(0);
      for (int i = 0; i < NUM_PUBLISHERS; i++) {
         final int pubId = i;
         final Mqtt5BlockingClient publisher = publishers.get(i);
         publisherExecutor.execute(() -> {
            for (int seq = 0; seq < NUM_MESSAGES; seq++) {
               String payload = pubId + "-" + seq;
               try {
                  waitForClientConnected(publisher);
                  Mqtt5PublishResult result = publisher.publishWith()
                     .topic(TOPIC)
                     .qos(MqttQos.EXACTLY_ONCE)
                     .payload(payload.getBytes(StandardCharsets.UTF_8))
                     .send();
                  if (result.getError().isPresent()) {
                     throw result.getError().get();
                  }
               } catch (Throwable e) {
                  publishErrors.incrementAndGet();
                  logger.info("Pub failed: {}; in-flight QoS 2 state will be resumed on reconnect", payload, e);
               }
               sentMessages.add(payload);
            }
            logger.info("Publisher {} finished sending all {} messages", pubId, NUM_MESSAGES);
         });
      }

      // start broker restart task
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      ScheduledFuture restarter = scheduler.scheduleWithFixedDelay(() -> {
         try {
            logger.info("Stopping broker");
            server.stop();
            waitForServerToStop(server);

            logger.info("===========");
            for (Map.Entry<String, Set<String>> entry : receivedPerSubscriber.entrySet()) {
               logger.info("Subscriber {} received {}/{} messages", entry.getKey(), entry.getValue().size(), NUM_PUBLISHERS * NUM_MESSAGES);
            }
            logger.info("Last message received {}ms ago.", System.currentTimeMillis() - lastReceiveTime.get());
            logger.info("===========");

            // compacting keeps the journal small to reduce start-up time
            logger.info("Compacting journal...");
            compactJournal(server.getConfiguration().getJournalLocation(), server.getConfiguration().getJournalRetentionLocation(), ACTIVEMQ_DATA, "amq", server.getConfiguration().getJournalMinFiles(),
                           server.getConfiguration().getJournalPoolFiles(), server.getConfiguration().getJournalFileSize(), null, JournalRecordIds.UPDATE_DELIVERY_COUNT,
                           JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME);
            logger.info("Compacted journal.");

            server.start();
            waitForServerToStart(server);
         } catch (Exception e) {
            logger.warn("Error during broker restart", e);
         }
      }, RESTART_PAUSE, RESTART_PAUSE, TimeUnit.MILLISECONDS);

      // wait for all publishers to finish sending
      publisherExecutor.shutdown();
      assertTrue(publisherExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Publishers did not finish in time");
      logger.info("All publishers finished. Total messages sent: {}. Publish errors: {}", sentMessages.size(), publishErrors.get());

      // wait for all subscribers to receive all messages
      final long STALL_TIMEOUT_MS = 20_000;
      Wait.assertTrue(() -> {
         if (System.currentTimeMillis() - lastReceiveTime.get() > STALL_TIMEOUT_MS) {
            for (Map.Entry<String, Set<String>> entry : receivedPerSubscriber.entrySet()) {
               logger.warn("Subscriber {} received {}/{} messages", entry.getKey(), entry.getValue().size(), NUM_PUBLISHERS * NUM_MESSAGES);
            }
            throw new AssertionError("No subscriber has received a message in " + STALL_TIMEOUT_MS / 1000 + " seconds");
         }
         for (Map.Entry<String, Set<String>> duplicates : duplicatesPerSubscriber.entrySet()) {
            assertEquals(0, duplicates.getValue().size(), "Subscriber " + duplicates.getKey() + " received duplicates: " + duplicates.getValue());
         }
         for (Set<String> received : receivedPerSubscriber.values()) {
            if (received.size() < NUM_PUBLISHERS * NUM_MESSAGES) {
               return false;
            }
         }
         return true;
      }, TIMEOUT_SECONDS * 1000L, 100);

      disableProtocolLogging();

      // stop reconnection task, ensure broker is running
      restarter.cancel(true);
      scheduler.shutdownNow();
      scheduler.awaitTermination(10, TimeUnit.SECONDS);
      if (!server.isStarted()) {
         server.start();
         waitForServerToStart(server);
      }

      // verify all expected messages received with no duplicates
      for (Mqtt5BlockingClient subscriber : subscribers) {
         String clientId = getClientId(subscriber);
         assertEquals(0, duplicatesPerSubscriber.get(clientId).size(), "Subscriber " + clientId + " received duplicates: " + duplicatesPerSubscriber.get(clientId));
         assertEquals(NUM_PUBLISHERS * NUM_MESSAGES, receivedPerSubscriber.get(clientId).size(), "Subscriber " + clientId + " didn't receive: " + getMissingMessages(sentMessages, receivedPerSubscriber.get(clientId)));
         assertEquals(0L, getSubscriptionQueue(TOPIC, clientId).getMessageCount(), "Subscription queue for " + clientId + " has incorrect message count");
         assertEquals(0, getProtocolManager().getStateManager().getPacketIdCorrelationSize(clientId));
         assertEquals(0, getSubCacheSize(clientId));
         cleanDisconnect(subscriber);
         assertNull(getSubCache(clientId), "Sub cache should be null after clean start for " + clientId);
      }

      for (Mqtt5BlockingClient publisher : publishers) {
         String clientId = getClientId(publisher);
         Wait.assertEquals(0, () -> getPubCacheSize(clientId), 5000, 100);
         cleanDisconnect(publisher);
         assertNull(getPubCache(clientId), "Pub cache should be null after clean start for " + clientId);
      }
   }

   private Set<String> getMissingMessages(Set<String> expected, Set<String> received) {
      Set<String> missing = new HashSet<>(expected);
      missing.removeAll(received);
      return missing;
   }

   private static String getClientId(Mqtt5BlockingClient subscriber) {
      return subscriber.getConfig().getClientIdentifier().get().toString();
   }

   private Mqtt5BlockingClient createHiveMQClient(String clientId, boolean autoReconnect) {
      var builder = Mqtt5Client.builder()
         .identifier(clientId)
         .serverHost("localhost")
         .serverPort(MQTT_PORT);
      if (autoReconnect) {
         builder.automaticReconnect()
            .initialDelay(500, TimeUnit.MILLISECONDS)
            .maxDelay(500, TimeUnit.MILLISECONDS)
            .applyAutomaticReconnect();
      }
      return builder.buildBlocking();
   }

   private static void waitForClientConnected(Mqtt5BlockingClient client) {
      Wait.waitFor(() -> client.getConfig().getState().isConnected(), 30_000, 500);
   }

   private static void cleanDisconnect(Mqtt5BlockingClient client) {
      logger.info("cleanDisconnect for {}", getClientId(client));
      try {
         if (client.getConfig().getState().isConnected()) {
            client.disconnect();
         }
         client.connectWith().cleanStart(true).sessionExpiryInterval(0).send();
         client.disconnect();
      } catch (Exception e) {
         logger.debug("Error disconnecting: {}", e.getMessage());
      }
   }

   private MQTTProtocolManager getProtocolManager() {
      Acceptor acceptor = server.getRemotingService().getAcceptor(MQTT_PROTOCOL_NAME);
      if (acceptor instanceof AbstractAcceptor abstractAcceptor) {
         ProtocolManager protocolManager = abstractAcceptor.getProtocolMap().get(MQTT_PROTOCOL_NAME);
         if (protocolManager instanceof MQTTProtocolManager mqttProtocolManager) {
            return mqttProtocolManager;
         }
      }
      return null;
   }

   private DuplicateIDCache getPubCache(String clientId) {
      return getCache(clientId, MQTTPacketIdCache.TYPE.PUB);
   }

   private int getPubCacheSize(String clientId) {
      DuplicateIDCache cache = getPubCache(clientId);
      return cache == null ? 0 : cache.getMap().size();
   }

   private DuplicateIDCache getSubCache(String clientId) {
      return getCache(clientId, MQTTPacketIdCache.TYPE.SUB);
   }

   private int getSubCacheSize(String clientId) {
      DuplicateIDCache cache = getSubCache(clientId);
      return cache == null ? 0 : cache.getMap().size();
   }

   private DuplicateIDCache getCache(String clientId, MQTTPacketIdCache.TYPE type) {
      SimpleString cacheName = MQTTPacketIdCache.getCacheName(server.getInternalNamingPrefix(), clientId, type);
      if (server.getPostOffice().duplicateIDCacheExists(cacheName)) {
         return server.getPostOffice().getDuplicateIDCache(cacheName);
      }
      return null;
   }

   private Queue getSubscriptionQueue(String mqttTopicFilter, String clientId) {
      return server.locateQueue(MQTTUtil.getCoreQueueFromMqttTopic(mqttTopicFilter, clientId, server.getConfiguration().getWildcardConfiguration()));
   }
}