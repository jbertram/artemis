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
package org.apache.activemq.artemis.tests.integration.paging;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A removal emptying the underlying {@code TreeMap} concurrently with an insertion corrupts the tree, which results in
 * a {@code NullPointerException} out of {@code TreeMap.fixAfterInsertion}/{@code fixAfterDeletion} or as a permanent
 * spin inside {@code TreeMap.getEntry} once the tree ends up with a cycle.
 * <p>
 * Because one of the failure modes is an unbreakable CPU spin, the test is written so that a regression still fails
 * rather than wedging the build:
 * <ul>
 * <li>the worker threads are daemons, so a spinning worker cannot keep the fork alive</li>
 * <li>{@link Timeout.ThreadMode#SEPARATE_THREAD} lets the test abort even if a worker never finishes</li>
 * <li>broker-driven cleanup is disabled so that the only threads touching {@code consumedPages} are the two workers.
 * {@code PageCursorProviderImpl.cleanup()} holds a storage manager read lock while it walks the subscriptions, so a
 * paging thread stuck on the corrupted map would otherwise deadlock {@code server.stop()} during teardown.</li>
 * </ul>
 */
public class PageSubscriptionCleanupConcurrencyTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int PAGE_SIZE = 10 * 1024;
   private static final int PAGE_MAX = 100 * 1024;

   // page numbers far beyond anything the store ever wrote, so cleanupEntries always considers them non existent
   private static final long FIRST_FAKE_PAGE = 1_000;
   private static final int FAKE_PAGES_PER_ROUND = 50;
   private static final int ROUNDS = 2_000;

   @Test
   @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
   public void testCleanupEntriesConcurrentWithPageInfoCreation() throws Exception {
      Configuration config = createDefaultConfig(true).setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<>());
      server.start();

      SimpleString queueName = SimpleString.of(getName());
      Queue queue = server.createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));

      queue.getPagingStore().startPaging();
      assertTrue(queue.getPagingStore().isPaging());

      PageSubscription subscription = queue.getPageSubscription();

      // keep the broker's own threads away from consumedPages, otherwise a regression deadlocks teardown instead of
      // failing the test
      subscription.disableAutoCleanup();
      queue.getPagingStore().getCursorProvider().disableCleanup();

      // cleanupEntries is a no-op until at least one ACK has been processed
      subscription.confirmPosition(new PagePositionImpl(1, 0));
      Wait.assertTrue(() -> subscription.locatePageInfo(1) != null, 5_000, 10);

      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final CountDownLatch start = new CountDownLatch(1);
      final CountDownLatch done = new CountDownLatch(2);
      final AtomicBoolean adderFinished = new AtomicBoolean(false);

      Thread adder = new Thread(() -> {
         try {
            start.await();
            for (int round = 0; round < ROUNDS; round++) {
               for (int i = 0; i < FAKE_PAGES_PER_ROUND; i++) {
                  subscription.reloadPageInfo(FIRST_FAKE_PAGE + i);
               }
            }
         } catch (Throwable t) {
            logger.debug("adder failed", t);
            failure.compareAndSet(null, t);
         } finally {
            adderFinished.set(true);
            done.countDown();
         }
      }, "page-info-adder");
      adder.setDaemon(true);

      Thread cleaner = new Thread(() -> {
         try {
            start.await();
            while (!adderFinished.get()) {
               subscription.cleanupEntries(false);
            }
         } catch (Throwable t) {
            logger.debug("cleaner failed", t);
            failure.compareAndSet(null, t);
         } finally {
            done.countDown();
         }
      }, "page-info-cleaner");
      cleaner.setDaemon(true);

      adder.start();
      cleaner.start();
      start.countDown();

      // the workers normally finish in well under a second, so this is a generous budget; it only expires when the tree
      // has been corrupted into a cycle and a worker is spinning
      boolean finished = done.await(10, TimeUnit.SECONDS);

      if (failure.get() != null) {
         fail("concurrent access to consumedPages failed: " + failure.get(), failure.get());
      }

      assertTrue(finished, "concurrent page cursor access did not finish in time, the consumedPages tree is likely corrupted");
   }
}
