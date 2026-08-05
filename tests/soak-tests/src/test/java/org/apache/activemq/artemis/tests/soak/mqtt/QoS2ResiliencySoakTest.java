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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTPacketIdCache;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTProtocolManager;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTUtil;
import org.apache.activemq.artemis.core.remoting.impl.AbstractAcceptor;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.TestParameters;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.eclipse.paho.mqttv5.client.DisconnectedBufferOptions;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.client.MqttClientException;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.core.protocol.mqtt.MQTTProtocolManagerFactory.MQTT_PROTOCOL_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QoS2ResiliencySoakTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final java.util.logging.Logger PAHO_LOGGER;

   static {
      PAHO_LOGGER = java.util.logging.Logger.getLogger("org.eclipse.paho.mqttv5.client.internal.ClientState");
      PAHO_LOGGER.setLevel(java.util.logging.Level.WARNING);
   }

   private static final String TEST_NAME = "QOS2_RESILIENCY_SOAK";
   private static final String TOPIC = "qos2/resiliency";
   private static final int EXACTLY_ONCE = 2;
   private static final int MQTT_PORT = 1883;

   private static final int NUM_PUBLISHERS = TestParameters.testProperty(TEST_NAME, "NUM_PUBLISHERS", 10);
   private static final int NUM_SUBSCRIBERS = TestParameters.testProperty(TEST_NAME, "NUM_SUBSCRIBERS", 10);
   private static final int NUM_MESSAGES = TestParameters.testProperty(TEST_NAME, "NUM_MESSAGES", 50_000);
   private static final int RESTART_PAUSE = TestParameters.testProperty(TEST_NAME, "RESTART_PAUSE", 3_000);
   private static final int TIMEOUT_SECONDS = TestParameters.testProperty(TEST_NAME, "TIMEOUT_SECONDS", 2400);

   private ActiveMQServer server;

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, createDefaultConfig(true));
      server.getConfiguration().addAcceptorConfiguration(MQTT_PROTOCOL_NAME, "tcp://localhost:" + MQTT_PORT + "?protocols=MQTT");
      server.getConfiguration().setMqttSessionScanInterval(200);
      server.getConfiguration().setSecurityEnabled(false);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAutoCreateQueues(true);
      addressSettings.setAutoCreateAddresses(true);
      server.getAddressSettingsRepository().addMatch("#", addressSettings);

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
      super.tearDown();
   }

   @Test
   @Timeout(value = 30, unit = TimeUnit.MINUTES)
   public void testQoS2PublisherResiliency() throws Exception {
      final String PUB_CLIENT_ID_PREFIX = "pub-";
      final String SUB_CLIENT_ID = "sub";

      // create subscription queue for consuming messages later
      MqttClient subscriber = createPahoClient(SUB_CLIENT_ID);
      MqttConnectionOptions subscriberOptions = new MqttConnectionOptionsBuilder()
         .cleanStart(false)
         .sessionExpiryInterval(300L)
         .build();
      subscriber.connect(subscriberOptions);
      subscriber.subscribe(TOPIC, 1);
      subscriber.disconnect();

      assertNotNull(getSubscriptionQueue(TOPIC, SUB_CLIENT_ID));

      logger.info("{} publishers, {} messages/publisher, {} total expected", NUM_PUBLISHERS, NUM_MESSAGES, NUM_PUBLISHERS * NUM_MESSAGES);

      // create and connect publishers
      final List<MqttClient> publishers = new ArrayList<>();
      runAfter(() -> publishers.forEach(c -> {
         try {
            c.disconnectForcibly(0, 0);
            c.close();
         } catch (Exception ignored) {
         }
      }));
      for (int i = 0; i < NUM_PUBLISHERS; i++) {
         String clientId = PUB_CLIENT_ID_PREFIX + i;
         MqttClient publisher = createPahoClient(clientId);
         MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
            .cleanStart(false)
            .sessionExpiryInterval(300L)
            .automaticReconnect(true)
            .build();
         options.setMaxReconnectDelay(500);
         publisher.connect(options);
         publishers.add(publisher);
         logger.info("Publisher {} connected", clientId);
      }

      // Start broker restart task
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      ScheduledFuture restarter = scheduler.scheduleWithFixedDelay(() -> {
         try {
            logger.info("Stopping");
            server.stop();
            waitForServerToStop(server);
            server.start();
            waitForServerToStart(server);
         } catch (Exception e) {
            logger.warn("Error during broker restart", e);
         }
      }, RESTART_PAUSE, RESTART_PAUSE, TimeUnit.MILLISECONDS);

      // enable protocol logging for the critical part of the test
      enableProtocolLogging();

      // Start publisher tasks
      final ExecutorService publisherExecutor = Executors.newFixedThreadPool(NUM_PUBLISHERS);
      runAfter(publisherExecutor::shutdownNow);
      final Set<String> sentMessages = ConcurrentHashMap.newKeySet(NUM_PUBLISHERS * NUM_MESSAGES);
      final AtomicInteger publishErrors = new AtomicInteger(0);
      for (int i = 0; i < NUM_PUBLISHERS; i++) {
         final int pubId = i;
         final MqttClient publisher = publishers.get(i);
         publisherExecutor.execute(() -> {
            for (int seq = 0; seq < NUM_MESSAGES; seq++) {
               String payload = PUB_CLIENT_ID_PREFIX + pubId + "-" + seq;
               while (!sentMessages.contains(payload)) {
                  try {
                     waitForClientConnected(publisher);
                     publisher.publish(TOPIC, payload.getBytes(StandardCharsets.UTF_8), EXACTLY_ONCE, false);
                     sentMessages.add(payload);
                  } catch (MqttException e) {
                     publishErrors.incrementAndGet();
                     if (e.getReasonCode() == MqttClientException.REASON_CODE_CLIENT_NOT_CONNECTED) {
                        logger.info("Pub failed: {}; client not connected, retrying manually", payload, e);
                     } else {
                        sentMessages.add(payload);
                        logger.info("Pub failed: {}; message should be retried by client automatically", payload);
                     }
                  }
               }
            }
            logger.info("Publisher pub-{}: finished sending all {} messages", pubId, NUM_MESSAGES);
         });
      }

      // wait for all publishers to finish
      publisherExecutor.shutdown();
      assertTrue(publisherExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Publishers did not finish in time");
      logger.info("All publishers finished. Total messages sent: {}. Publish errors: {}", sentMessages.size(), publishErrors.get());

      for (MqttClient publisher : publishers) {
         safeDisconnect(publisher);
      }

      disableProtocolLogging();

      // stop restart task and ensure broker is running
      restarter.cancel(true);
      scheduler.shutdownNow();
      if (!server.isStarted()) {
         server.start();
         waitForServerToStart(server);
      }

      long messageCount = getSubscriptionQueue(TOPIC, SUB_CLIENT_ID).getMessageCount();

      logger.info("Subscription queue message count: {}", messageCount);

      // reconnect subscriber to verify there are no duplicates
      Set<String> consumedMessages = ConcurrentHashMap.newKeySet(NUM_MESSAGES * NUM_PUBLISHERS);
      AtomicInteger duplicateCount = new AtomicInteger(0);
      subscriber.setCallback(new MQTT5SoakTest.DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            if (consumedMessages.contains(payload)) {
               logger.warn("Duplicate message: {}", payload);
               duplicateCount.incrementAndGet();
            }
            consumedMessages.add(payload);
            sentMessages.remove(payload);
            logger.info("Consumed: {}", payload);
         }
      });
      subscriber.connect(subscriberOptions);

      Wait.assertEquals(0, () -> getSubscriptionQueue(TOPIC, SUB_CLIENT_ID).getMessageCount());
      Wait.waitFor(() -> consumedMessages.size() == messageCount);
      assertEquals(0, duplicateCount.get());
      assertEquals(0, sentMessages.size(), "These messages were published, but were not on the broker: " + sentMessages);
      assertEquals(NUM_PUBLISHERS * NUM_MESSAGES, consumedMessages.size());

      // verify QoS2 state is clean
      for (int i = 0; i < NUM_PUBLISHERS; i++) {
         String clientId = PUB_CLIENT_ID_PREFIX + i;
         Wait.assertEquals(0, () -> getPubCacheSize(clientId), 5000, 100);
      }
      logger.info("QoS2 state verified clean for all clients");

      // clean session teardown
      for (MqttClient publisher : publishers) {
         safeDisconnect(publisher);
         publisher.connect(new MqttConnectionOptionsBuilder().cleanStart(true).sessionExpiryInterval(0L).build());
         String clientId = publisher.getClientId();
         assertNull(getPubCache(clientId), "Pub cache should be null after clean start for " + clientId);
         publisher.disconnect();
         publisher.close();
      }
   }

   private MqttClient createPahoClient(String clientId) throws MqttException {
      return new MqttClient("tcp://localhost:" + MQTT_PORT, clientId, new MemoryPersistence());
   }

   private static void waitForClientConnected(MqttClient client) {
      Wait.waitFor(() -> {
         //         logger.info("Is {} connected?", client);
         return client.isConnected();
      }, 30_000, 500);
   }

   private static void safeDisconnect(MqttClient client) {
      try {
         if (client.isConnected()) {
            client.disconnect();
         }
      } catch (MqttException e) {
         logger.debug("Error disconnecting {}: {}", client.getClientId(), e.getMessage());
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
