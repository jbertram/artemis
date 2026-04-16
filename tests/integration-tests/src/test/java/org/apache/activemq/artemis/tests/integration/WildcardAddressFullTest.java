/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.activemq.artemis.tests.integration;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

public class WildcardAddressFullTest extends ActiveMQTestBase {

   protected ActiveMQServer server;
   protected ClientSession session;
   protected ClientSessionFactory sf;
   protected ServerLocator locator;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(false, createDefaultInVMConfig());
      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }

   @Test
   public void testWorking() throws Exception {
      test("a.b", "a.b", "a.#");
   }

   @Test
   public void testFailing() throws Exception {
      test("a.b", "a.#", "a.#");
   }

   private void test(final String addressToSend, final String queueToReceive, final String addressSettingsMatch) throws Exception {
      server.getAddressSettingsRepository().addMatch(addressSettingsMatch, new AddressSettings().setMaxSizeMessages(1).setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL));

      session.createQueue(QueueConfiguration.of(queueToReceive).setRoutingType(RoutingType.MULTICAST));
      session.createAddress(SimpleString.of(addressToSend), RoutingType.MULTICAST, false);

      ClientProducer producer = session.createProducer(addressToSend);

      ClientMessage message = session.createMessage(true);
      message.setRoutingType(RoutingType.MULTICAST);

      producer.send(message);

      try {
         producer.send(message);
         fail("should have failed here");
      } catch (Exception e) {
         // ignore
      }
   }
}
