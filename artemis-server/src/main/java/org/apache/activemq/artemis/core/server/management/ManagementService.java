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
package org.apache.activemq.artemis.core.server.management;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import javax.management.ObjectName;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.AcceptorControl;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.BrokerConnectionControl;
import org.apache.activemq.artemis.api.core.management.ConnectionRouterControl;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.messagecounter.MessageCounterManager;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.BrokerConnection;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.RemoteBrokerConnection;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.cluster.BroadcastGroup;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.routing.ConnectionRouter;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;

public interface ManagementService extends NotificationService, ActiveMQComponent {

   MessageCounterManager getMessageCounterManager();

   SimpleString getManagementAddress();

   SimpleString getManagementNotificationAddress();

   ObjectNameBuilder getObjectNameBuilder();

   void setStorageManager(StorageManager storageManager);

   void registerInJMX(ObjectName objectName, Object managedResource) throws Exception;

   void unregisterFromJMX(ObjectName objectName) throws Exception;

   ActiveMQServerControlImpl registerServer(PostOffice postOffice,
                                            SecurityStore securityStore,
                                            StorageManager storageManager,
                                            Configuration configuration,
                                            HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                            HierarchicalRepository<Set<Role>> securityRepository,
                                            ResourceManager resourceManager,
                                            RemotingService remotingService,
                                            ActiveMQServer messagingServer,
                                            QueueFactory queueFactory,
                                            ScheduledExecutorService scheduledThreadPool,
                                            PagingManager pagingManager,
                                            boolean backup) throws Exception;

   void unregisterServer() throws Exception;

   ActiveMQServerControl getServerControl();

   void registerAddress(AddressInfo addressInfo) throws Exception;

   void unregisterAddress(SimpleString address) throws Exception;

   int getAddressControlCount();

   List<AddressControl> getAddressControls();

   List<AddressControl> getAddressControls(Predicate<AddressControl> predicate);

   AddressControl getAddressControl(String name);

   List<String> getAddressControlNames();

   void registerQueue(Queue queue, SimpleString address, StorageManager storageManager) throws Exception;

   void unregisterQueue(SimpleString name, SimpleString address, RoutingType routingType) throws Exception;

   int getQueueControlCount();

   List<QueueControl> getQueueControls();

   List<QueueControl> getQueueControls(Predicate<QueueControl> predicate);

   QueueControl getQueueControl(String name);

   List<String> getQueueControlNames();

   void registerAcceptor(Acceptor acceptor, TransportConfiguration configuration) throws Exception;

   void unregisterAcceptor(String acceptorName) throws Exception;

   void unregisterAcceptor();

   AcceptorControl getAcceptorControl(String name);

   void registerDivert(Divert divert) throws Exception;

   void unregisterDivert(SimpleString name, SimpleString address) throws Exception;

   List<DivertControl> getDivertControls();

   List<String> getDivertControlNames();

   void registerBroadcastGroup(BroadcastGroup broadcastGroup, BroadcastGroupConfiguration configuration) throws Exception;

   void unregisterBroadcastGroup(String name) throws Exception;

   void registerBridge(Bridge bridge) throws Exception;

   void unregisterBridge(String name) throws Exception;

   List<BridgeControl> getBridgeControls();

   List<String> getBridgeControlNames();

   int getBridgeControlCount();

   void registerCluster(ClusterConnection cluster, ClusterConnectionConfiguration configuration) throws Exception;

   void unregisterCluster(String name) throws Exception;

   void registerConnectionRouter(ConnectionRouter router) throws Exception;

   void unregisterConnectionRouter(String name) throws Exception;

   ConnectionRouterControl getConnectionRouterControl(String name);

   void registerBrokerConnection(BrokerConnection brokerConnection) throws Exception;

   void unregisterBrokerConnection(String name) throws Exception;

   BrokerConnectionControl getBrokerConnectionControl(String name);

   void registerRemoteBrokerConnection(RemoteBrokerConnection brokerConnection) throws Exception;

   void unregisterRemoteBrokerConnection(String nodeId, String name) throws Exception;

   void registerHawtioSecurity(GuardInvocationHandler guardInvocationHandler) throws Exception;

   void unregisterHawtioSecurity() throws Exception;

   void registerUntypedControl(String name, Object control);

   void unregisterUntypedControl(String name);

   Object getUntypedControl(String name);

   ICoreMessage handleMessage(SecurityAuth auth, Message message) throws Exception;

   Object getAttribute(String resourceName, String attribute, SecurityAuth auth);

   Object invokeOperation(String resourceName, String operation, Object[] params, SecurityAuth auth) throws Exception;
}
