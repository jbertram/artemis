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
package org.apache.activemq.artemis.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAJMSContext;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;
import javax.naming.Context;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jndi.JNDIStorable;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManagerFactory;
import org.apache.activemq.artemis.uri.ConnectionFactoryParser;
import org.apache.activemq.artemis.uri.ServerLocatorParser;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.apache.activemq.artemis.utils.sm.SecurityManagerShim;
import org.apache.activemq.artemis.utils.uri.BeanSupport;
import org.apache.activemq.artemis.utils.uri.URISupport;

/**
 * ActiveMQ Artemis implementation of a JMS ConnectionFactory.
 * <p>
 * This connection factory will use defaults defined by {@link DefaultConnectionProperties}.
 */
public class ActiveMQConnectionFactory extends JNDIStorable implements ConnectionFactoryOptions, Externalizable, ConnectionFactory, XAConnectionFactory, AutoCloseable {

   private static final long serialVersionUID = 6730844785641767519L;

   private ServerLocator serverLocator;

   private String clientID;

   private boolean enableSharedClientID = ActiveMQClient.DEFAULT_ENABLED_SHARED_CLIENT_ID;

   private int dupsOKBatchSize = ActiveMQClient.DEFAULT_ACK_BATCH_SIZE;

   private int transactionBatchSize = ActiveMQClient.DEFAULT_ACK_BATCH_SIZE;

   private boolean readOnly;

   private String user;

   private String password;

   private String protocolManagerFactoryStr;

   private String deserializationDenyList;

   private String deserializationAllowList;

   private boolean cacheDestinations;

   private boolean ignoreJTA;

   private boolean enable1xPrefixes = ActiveMQJMSClient.DEFAULT_ENABLE_1X_PREFIXES;

   // keeping this field for serialization compatibility only. do not use it
   @SuppressWarnings("unused")
   private boolean finalizeCheck;

   private Lock lock = new ReentrantLock();

   @Override
   public void writeExternal(ObjectOutput out) throws IOException {
      URI uri = toURI();

      try {
         out.writeUTF(uri.toASCIIString());
      } catch (Exception e) {
         if (e instanceof IOException ioException) {
            throw ioException;
         }
         throw new IOException(e);
      }
   }

   public URI toURI() throws IOException {
      ConnectionFactoryParser parser = new ConnectionFactoryParser();
      String scheme;
      if (serverLocator.getDiscoveryGroupConfiguration() != null) {
         if (serverLocator.getDiscoveryGroupConfiguration().getBroadcastEndpointFactory() instanceof UDPBroadcastEndpointFactory) {
            scheme = "udp";
         } else {
            scheme = "jgroups";
         }
      } else {
         if (serverLocator.allInVM()) {
            scheme = "vm";
         } else {
            scheme = "tcp";
         }
      }

      URI uri;

      try {
         uri = parser.createSchema(scheme, this);
      } catch (Exception e) {
         if (e instanceof IOException ioException) {
            throw ioException;
         }
         throw new IOException(e);
      }
      return uri;
   }

   public String getProtocolManagerFactoryStr() {
      return protocolManagerFactoryStr;
   }

   public void setProtocolManagerFactoryStr(final String protocolManagerFactoryStr) {

      if (protocolManagerFactoryStr != null && !protocolManagerFactoryStr.trim().isEmpty() &&
         !protocolManagerFactoryStr.equals("undefined")) {
         SecurityManagerShim.doPrivileged((PrivilegedAction<Object>) () -> {
            ClientProtocolManagerFactory protocolManagerFactory = (ClientProtocolManagerFactory) ClassloadingUtil.newInstanceFromClassLoader(ActiveMQConnectionFactory.class, protocolManagerFactoryStr, ClientProtocolManagerFactory.class);
            serverLocator.setProtocolManagerFactory(protocolManagerFactory);
            return null;
         });

         this.protocolManagerFactoryStr = protocolManagerFactoryStr;
      }
   }

   @Override
   @Deprecated(forRemoval = true)
   public String getDeserializationBlackList() {
      return deserializationDenyList;
   }

   @Override
   @Deprecated(forRemoval = true)
   public void setDeserializationBlackList(String denyList) {
      this.deserializationDenyList = denyList;
   }

   @Override
   @Deprecated(forRemoval = true)
   public String getDeserializationWhiteList() {
      return deserializationAllowList;
   }

   @Override
   @Deprecated(forRemoval = true)
   public void setDeserializationWhiteList(String allowList) {
      this.deserializationAllowList = allowList;
   }

   @Override
   public String getDeserializationDenyList() {
      return deserializationDenyList;
   }

   @Override
   public void setDeserializationDenyList(String denyList) {
      this.deserializationDenyList = denyList;
   }

   @Override
   public String getDeserializationAllowList() {
      return deserializationAllowList;
   }

   @Override
   public void setDeserializationAllowList(String allowList) {
      this.deserializationAllowList = allowList;
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      String url = in.readUTF();
      ConnectionFactoryParser parser = new ConnectionFactoryParser();
      ServerLocatorParser locatorParser = new ServerLocatorParser();
      try {
         URI uri = new URI(url);
         serverLocator = locatorParser.newObject(uri, null);
         parser.populateObject(uri, this);
      } catch (Exception e) {
         InvalidObjectException ex = new InvalidObjectException(e.getMessage());
         ex.initCause(e);
         throw ex;
      }
   }

   /**
    * This will use a default URI from {@link DefaultConnectionProperties}
    */
   public ActiveMQConnectionFactory() {
      this(DefaultConnectionProperties.DEFAULT_BROKER_URL);
   }

   public ActiveMQConnectionFactory(String brokerURL) {
      try {
         setBrokerURL(brokerURL);
      } catch (Throwable e) {
         throw new IllegalStateException(e);
      }
   }

   /**
    * <b>Warning</b>: This method will not clear any previous properties. For example, if you set the user first then
    * you change the brokerURL without passing the user. The previously set user will still exist, and nothing will
    * clear it out.
    * <p>
    * Also, you cannot use this method after this {@code ConnectionFactory} is made {@code readOnly} which happens after
    * the first time it's used to create a connection.
    */
   public void setBrokerURL(String brokerURL) throws JMSException {
      if (readOnly) {
         throw new javax.jms.IllegalStateException("You cannot use setBrokerURL after the connection factory has been used");
      }
      ConnectionFactoryParser cfParser = new ConnectionFactoryParser();
      try {
         URI uri = cfParser.expandURI(brokerURL);
         serverLocator = ServerLocatorImpl.newLocator(uri);
         cfParser.populateObject(uri, this);
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }

      if (getUser() == null) {
         setUser(DefaultConnectionProperties.DEFAULT_USER);
      }

      if (getPassword() == null) {
         setPassword(DefaultConnectionProperties.DEFAULT_PASSWORD);
      }

      if (getPasswordCodec() == null) {
         setPasswordCodec(DefaultConnectionProperties.DEFAULT_PASSWORD_CODEC);
      }
   }

   /**
    * For compatibility and users used to this kind of constructor
    */
   public ActiveMQConnectionFactory(String url, String user, String password) {
      this(url);
      setUser(user).setPassword(password);
   }

   public ActiveMQConnectionFactory(final ServerLocator serverLocator) {
      this.serverLocator = serverLocator;
   }

   public ActiveMQConnectionFactory(final boolean ha, final DiscoveryGroupConfiguration groupConfiguration) {
      if (ha) {
         serverLocator = ActiveMQClient.createServerLocatorWithHA(groupConfiguration);
      } else {
         serverLocator = ActiveMQClient.createServerLocatorWithoutHA(groupConfiguration);
      }
   }

   public ActiveMQConnectionFactory(final boolean ha, final TransportConfiguration... initialConnectors) {
      if (ha) {
         serverLocator = ActiveMQClient.createServerLocatorWithHA(initialConnectors);
      } else {
         serverLocator = ActiveMQClient.createServerLocatorWithoutHA(initialConnectors);
      }
   }

   @Override
   public Connection createConnection() throws JMSException {
      return createConnection(user, password);
   }

   @Override
   public Connection createConnection(final String username, final String password) throws JMSException {
      return createConnectionInternal(username, password, false, ActiveMQConnection.TYPE_GENERIC_CONNECTION);
   }

   @Override
   public JMSContext createContext() {
      return createContext(user, password);
   }

   @Override
   public JMSContext createContext(final int sessionMode) {
      return createContext(user, password, sessionMode);
   }

   @Override
   public JMSContext createContext(final String userName, final String password) {
      return createContext(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
   }

   @Override
   public JMSContext createContext(String userName, String password, int sessionMode) {
      validateSessionMode(sessionMode);
      try {
         ActiveMQConnection connection = createConnectionInternal(userName, password, false, ActiveMQConnection.TYPE_GENERIC_CONNECTION);
         return connection.createContext(sessionMode);
      } catch (JMSSecurityException e) {
         throw new JMSSecurityRuntimeException(e.getMessage(), e.getErrorCode(), e);
      } catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   private static void validateSessionMode(int mode) {
      switch (mode) {
         case JMSContext.AUTO_ACKNOWLEDGE:
         case JMSContext.CLIENT_ACKNOWLEDGE:
         case JMSContext.DUPS_OK_ACKNOWLEDGE:
         case JMSContext.SESSION_TRANSACTED:
         case ActiveMQJMSConstants.PRE_ACKNOWLEDGE:
         case ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE: {
            return;
         }
         default:
            throw new JMSRuntimeException("Invalid Session Mode: " + mode);
      }
   }

   public QueueConnection createQueueConnection() throws JMSException {
      return createQueueConnection(user, password);
   }

   public QueueConnection createQueueConnection(final String username, final String password) throws JMSException {
      return createConnectionInternal(username, password, false, ActiveMQConnection.TYPE_QUEUE_CONNECTION);
   }

   // TopicConnectionFactory implementation --------------------------------------------------------

   public TopicConnection createTopicConnection() throws JMSException {
      return createTopicConnection(user, password);
   }

   public TopicConnection createTopicConnection(final String username, final String password) throws JMSException {
      return createConnectionInternal(username, password, false, ActiveMQConnection.TYPE_TOPIC_CONNECTION);
   }

   // XAConnectionFactory implementation -----------------------------------------------------------

   @Override
   public XAConnection createXAConnection() throws JMSException {
      return createXAConnection(user, password);
   }

   @Override
   public XAConnection createXAConnection(final String username, final String password) throws JMSException {
      return (XAConnection) createConnectionInternal(username, password, true, ActiveMQConnection.TYPE_GENERIC_CONNECTION);
   }

   @Override
   public XAJMSContext createXAContext() {
      return createXAContext(user, password);
   }

   @Override
   public XAJMSContext createXAContext(String userName, String password) {
      try {
         ActiveMQConnection connection = createConnectionInternal(userName, password, true, ActiveMQConnection.TYPE_GENERIC_CONNECTION);
         return connection.createXAContext();
      } catch (JMSSecurityException e) {
         throw new JMSSecurityRuntimeException(e.getMessage(), e.getErrorCode(), e);
      } catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   // XAQueueConnectionFactory implementation ------------------------------------------------------

   public XAQueueConnection createXAQueueConnection() throws JMSException {
      return createXAQueueConnection(user, password);
   }

   public XAQueueConnection createXAQueueConnection(final String username, final String password) throws JMSException {
      return (XAQueueConnection) createConnectionInternal(username, password, true, ActiveMQConnection.TYPE_QUEUE_CONNECTION);
   }

   // XATopicConnectionFactory implementation ------------------------------------------------------

   public XATopicConnection createXATopicConnection() throws JMSException {
      return createXATopicConnection(user, password);
   }

   public XATopicConnection createXATopicConnection(final String username, final String password) throws JMSException {
      return (XATopicConnection) createConnectionInternal(username, password, true, ActiveMQConnection.TYPE_TOPIC_CONNECTION);
   }

   @Override
   protected void buildFromProperties(Properties props) {
      String url = props.getProperty(Context.PROVIDER_URL);
      if (url == null || url.isEmpty()) {
         url = props.getProperty("brokerURL");
      }

      if (url == null || url.isEmpty()) {
         throw new IllegalArgumentException(Context.PROVIDER_URL + " or " + "brokerURL is required");
      }
      try {
         if (url != null && !url.isEmpty()) {
            url = updateBrokerURL(url, props);
            setBrokerURL(url);
         }

         BeanSupport.setProperties(this, props);
      } catch (JMSException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
         throw new RuntimeException(e);
      }
   }

   private String updateBrokerURL(String url, Properties props) {
      ConnectionFactoryParser cfParser = new ConnectionFactoryParser();
      try {
         URI uri = cfParser.expandURI(url);
         final Map<String, String> params = URISupport.parseParameters(uri);

         for (String key : TransportConstants.ALLOWABLE_CONNECTOR_KEYS) {
            final String val = props.getProperty(key);
            if (val != null) {
               params.put(key, val);
            }
         }

         final String newUrl = URISupport.applyParameters(uri, params).toString();
         return newUrl;
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   protected void populateProperties(Properties props) {
      try {
         URI uri = toURI();
         if (uri != null) {
            props.put(Context.PROVIDER_URL, uri.toASCIIString());
            props.put("brokerURL", uri.toASCIIString());
         }
         BeanSupport.getProperties(this, props);
      } catch (IOException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
         throw new RuntimeException(e);
      }
   }

   public boolean isHA() {
      return serverLocator.isHA();
   }

   public String getConnectionLoadBalancingPolicyClassName() {
      lock.lock();
      try {
         return serverLocator.getConnectionLoadBalancingPolicyClassName();
      } finally {
         lock.unlock();
      }
   }

   public void setConnectionLoadBalancingPolicyClassName(final String connectionLoadBalancingPolicyClassName) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setConnectionLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName);
      } finally {
         lock.unlock();
      }
   }

   public TransportConfiguration[] getStaticConnectors() {
      lock.lock();
      try {
         return serverLocator.getStaticTransportConfigurations();
      } finally {
         lock.unlock();
      }
   }

   public DiscoveryGroupConfiguration getDiscoveryGroupConfiguration() {
      lock.lock();
      try {
         return serverLocator.getDiscoveryGroupConfiguration();
      } finally {
         lock.unlock();
      }
   }

   public String getClientID() {
      lock.lock();
      try {
         return clientID;
      } finally {
         lock.unlock();
      }
   }

   public void setClientID(final String clientID) {
      lock.lock();
      try {
         checkWrite();
         this.clientID = clientID;
      } finally {
         lock.unlock();
      }
   }

   public boolean isEnableSharedClientID() {
      lock.lock();
      try {
         return enableSharedClientID;
      } finally {
         lock.unlock();
      }
   }

   public void setEnableSharedClientID(boolean enableSharedClientID) {
      lock.lock();
      try {
         this.enableSharedClientID = enableSharedClientID;
      } finally {
         lock.unlock();
      }
   }

   public int getDupsOKBatchSize() {
      lock.lock();
      try {
         return dupsOKBatchSize;
      } finally {
         lock.unlock();
      }
   }

   public void setDupsOKBatchSize(final int dupsOKBatchSize) {
      lock.lock();
      try {
         checkWrite();
         this.dupsOKBatchSize = dupsOKBatchSize;
      } finally {
         lock.unlock();
      }
   }

   public int getTransactionBatchSize() {
      lock.lock();
      try {
         return transactionBatchSize;
      } finally {
         lock.unlock();
      }
   }

   public void setTransactionBatchSize(final int transactionBatchSize) {
      lock.lock();
      try {
         checkWrite();
         this.transactionBatchSize = transactionBatchSize;
      } finally {
         lock.unlock();
      }
   }

   public boolean isCacheDestinations() {
      lock.lock();
      try {
         return this.cacheDestinations;
      } finally {
         lock.unlock();
      }
   }

   public void setCacheDestinations(final boolean cacheDestinations) {
      lock.lock();
      try {
         checkWrite();
         this.cacheDestinations = cacheDestinations;
      } finally {
         lock.unlock();
      }
   }

   public boolean isEnable1xPrefixes() {
      lock.lock();
      try {
         return this.enable1xPrefixes;
      } finally {
         lock.unlock();
      }
   }

   public void setEnable1xPrefixes(final boolean enable1xPrefixes) {
      lock.lock();
      try {
         checkWrite();
         this.enable1xPrefixes = enable1xPrefixes;
      } finally {
         lock.unlock();
      }
   }

   public long getClientFailureCheckPeriod() {
      lock.lock();
      try {
         return serverLocator.getClientFailureCheckPeriod();
      } finally {
         lock.unlock();
      }
   }

   public void setClientFailureCheckPeriod(final long clientFailureCheckPeriod) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      } finally {
         lock.unlock();
      }
   }

   public long getConnectionTTL() {
      lock.lock();
      try {
         return serverLocator.getConnectionTTL();
      } finally {
         lock.unlock();
      }
   }

   public void setConnectionTTL(final long connectionTTL) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setConnectionTTL(connectionTTL);
      } finally {
         lock.unlock();
      }
   }

   public long getCallTimeout() {
      lock.lock();
      try {
         return serverLocator.getCallTimeout();
      } finally {
         lock.unlock();
      }
   }

   public void setCallTimeout(final long callTimeout) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setCallTimeout(callTimeout);
      } finally {
         lock.unlock();
      }
   }

   public long getCallFailoverTimeout() {
      lock.lock();
      try {
         return serverLocator.getCallFailoverTimeout();
      } finally {
         lock.unlock();
      }
   }

   public void setCallFailoverTimeout(final long callTimeout) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setCallFailoverTimeout(callTimeout);
      } finally {
         lock.unlock();
      }
   }

   public void setUseTopologyForLoadBalancing(boolean useTopologyForLoadBalancing) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setUseTopologyForLoadBalancing(useTopologyForLoadBalancing);
      } finally {
         lock.unlock();
      }
   }

   public boolean isUseTopologyForLoadBalancing() {
      lock.lock();
      try {
         return serverLocator.getUseTopologyForLoadBalancing();
      } finally {
         lock.unlock();
      }
   }

   public int getConsumerWindowSize() {
      lock.lock();
      try {
         return serverLocator.getConsumerWindowSize();
      } finally {
         lock.unlock();
      }
   }

   public void setConsumerWindowSize(final int consumerWindowSize) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setConsumerWindowSize(consumerWindowSize);
      } finally {
         lock.unlock();
      }
   }

   public int getConsumerMaxRate() {
      lock.lock();
      try {
         return serverLocator.getConsumerMaxRate();
      } finally {
         lock.unlock();
      }
   }

   public void setConsumerMaxRate(final int consumerMaxRate) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setConsumerMaxRate(consumerMaxRate);
      } finally {
         lock.unlock();
      }
   }

   public int getConfirmationWindowSize() {
      lock.lock();
      try {
         return serverLocator.getConfirmationWindowSize();
      } finally {
         lock.unlock();
      }
   }

   public void setConfirmationWindowSize(final int confirmationWindowSize) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setConfirmationWindowSize(confirmationWindowSize);
      } finally {
         lock.unlock();
      }
   }

   public int getProducerMaxRate() {
      lock.lock();
      try {
         return serverLocator.getProducerMaxRate();
      } finally {
         lock.unlock();
      }
   }

   public void setProducerMaxRate(final int producerMaxRate) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setProducerMaxRate(producerMaxRate);
      } finally {
         lock.unlock();
      }
   }

   public int getProducerWindowSize() {
      lock.lock();
      try {
         return serverLocator.getProducerWindowSize();
      } finally {
         lock.unlock();
      }
   }

   public void setProducerWindowSize(final int producerWindowSize) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setProducerWindowSize(producerWindowSize);
      } finally {
         lock.unlock();
      }
   }

   public void setCacheLargeMessagesClient(final boolean cacheLargeMessagesClient) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setCacheLargeMessagesClient(cacheLargeMessagesClient);
      } finally {
         lock.unlock();
      }
   }

   public boolean isCacheLargeMessagesClient() {
      lock.lock();
      try {
         return serverLocator.isCacheLargeMessagesClient();
      } finally {
         lock.unlock();
      }
   }

   public int getMinLargeMessageSize() {
      lock.lock();
      try {
         return serverLocator.getMinLargeMessageSize();
      } finally {
         lock.unlock();
      }
   }

   public void setMinLargeMessageSize(final int minLargeMessageSize) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setMinLargeMessageSize(minLargeMessageSize);
      } finally {
         lock.unlock();
      }
   }

   public boolean isBlockOnAcknowledge() {
      lock.lock();
      try {
         return serverLocator.isBlockOnAcknowledge();
      } finally {
         lock.unlock();
      }
   }

   public void setBlockOnAcknowledge(final boolean blockOnAcknowledge) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setBlockOnAcknowledge(blockOnAcknowledge);
      } finally {
         lock.unlock();
      }
   }

   public boolean isBlockOnNonDurableSend() {
      lock.lock();
      try {
         return serverLocator.isBlockOnNonDurableSend();
      } finally {
         lock.unlock();
      }
   }

   public void setBlockOnNonDurableSend(final boolean blockOnNonDurableSend) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setBlockOnNonDurableSend(blockOnNonDurableSend);
      } finally {
         lock.unlock();
      }
   }

   public boolean isBlockOnDurableSend() {
      lock.lock();
      try {
         return serverLocator.isBlockOnDurableSend();
      } finally {
         lock.unlock();
      }
   }

   public void setBlockOnDurableSend(final boolean blockOnDurableSend) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setBlockOnDurableSend(blockOnDurableSend);
      } finally {
         lock.unlock();
      }
   }

   public boolean isAutoGroup() {
      lock.lock();
      try {
         return serverLocator.isAutoGroup();
      } finally {
         lock.unlock();
      }
   }

   public void setAutoGroup(final boolean autoGroup) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setAutoGroup(autoGroup);
      } finally {
         lock.unlock();
      }
   }

   public boolean isPreAcknowledge() {
      lock.lock();
      try {
         return serverLocator.isPreAcknowledge();
      } finally {
         lock.unlock();
      }
   }

   public void setPreAcknowledge(final boolean preAcknowledge) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setPreAcknowledge(preAcknowledge);
      } finally {
         lock.unlock();
      }
   }

   public long getRetryInterval() {
      lock.lock();
      try {
         return serverLocator.getRetryInterval();
      } finally {
         lock.unlock();
      }
   }

   public void setRetryInterval(final long retryInterval) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setRetryInterval(retryInterval);
      } finally {
         lock.unlock();
      }
   }

   public long getMaxRetryInterval() {
      lock.lock();
      try {
         return serverLocator.getMaxRetryInterval();
      } finally {
         lock.unlock();
      }
   }

   public void setMaxRetryInterval(final long retryInterval) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setMaxRetryInterval(retryInterval);
      } finally {
         lock.unlock();
      }
   }

   public double getRetryIntervalMultiplier() {
      lock.lock();
      try {
         return serverLocator.getRetryIntervalMultiplier();
      } finally {
         lock.unlock();
      }
   }

   public void setRetryIntervalMultiplier(final double retryIntervalMultiplier) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setRetryIntervalMultiplier(retryIntervalMultiplier);
      } finally {
         lock.unlock();
      }
   }

   public int getReconnectAttempts() {
      lock.lock();
      try {
         return serverLocator.getReconnectAttempts();
      } finally {
         lock.unlock();
      }
   }

   public void setReconnectAttempts(final int reconnectAttempts) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setReconnectAttempts(reconnectAttempts);
      } finally {
         lock.unlock();
      }
   }

   public void setInitialConnectAttempts(final int reconnectAttempts) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setInitialConnectAttempts(reconnectAttempts);
      } finally {
         lock.unlock();
      }
   }

   public int getInitialConnectAttempts() {
      return serverLocator.getInitialConnectAttempts();
   }

   @Deprecated
   public boolean isFailoverOnInitialConnection() {
      return false;
   }

   @Deprecated
   public void setFailoverOnInitialConnection(final boolean failover) {
   }

   public boolean isUseGlobalPools() {
      lock.lock();
      try {
         return serverLocator.isUseGlobalPools();
      } finally {
         lock.unlock();
      }
   }

   public void setUseGlobalPools(final boolean useGlobalPools) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setUseGlobalPools(useGlobalPools);
      } finally {
         lock.unlock();
      }
   }

   public int getScheduledThreadPoolMaxSize() {
      lock.lock();
      try {
         return serverLocator.getScheduledThreadPoolMaxSize();
      } finally {
         lock.unlock();
      }
   }

   public void setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
      } finally {
         lock.unlock();
      }
   }

   public int getThreadPoolMaxSize() {
      lock.lock();
      try {
         return serverLocator.getThreadPoolMaxSize();
      } finally {
         lock.unlock();
      }
   }

   public void setThreadPoolMaxSize(final int threadPoolMaxSize) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setThreadPoolMaxSize(threadPoolMaxSize);
      } finally {
         lock.unlock();
      }
   }

   public int getInitialMessagePacketSize() {
      lock.lock();
      try {
         return serverLocator.getInitialMessagePacketSize();
      } finally {
         lock.unlock();
      }
   }

   public void setInitialMessagePacketSize(final int size) {
      lock.lock();
      try {
         checkWrite();
         serverLocator.setInitialMessagePacketSize(size);
      } finally {
         lock.unlock();
      }
   }

   public boolean isIgnoreJTA() {
      return ignoreJTA;
   }

   public void setIgnoreJTA(boolean ignoreJTA) {
      checkWrite();
      this.ignoreJTA = ignoreJTA;
   }

   /**
    * Set the list of {@link Interceptor}s to use for incoming packets.
    *
    * @param interceptorList a comma separated string of incoming interceptor class names to be used. Each interceptor
    *                        needs a default Constructor to be used with this method.
    */
   public void setIncomingInterceptorList(String interceptorList) {
      checkWrite();
      serverLocator.setIncomingInterceptorList(interceptorList);
   }

   public String getIncomingInterceptorList() {
      return serverLocator.getIncomingInterceptorList();
   }

   /**
    * Set the list of {@link Interceptor}s to use for outgoing packets.
    *
    * @param interceptorList a comma separated string of incoming interceptor class names to be used. Each interceptor
    *                        needs a default Constructor to be used with this method.
    */
   public void setOutgoingInterceptorList(String interceptorList) {
      serverLocator.setOutgoingInterceptorList(interceptorList);
   }

   public String getOutgoingInterceptorList() {
      return serverLocator.getOutgoingInterceptorList();
   }

   public ActiveMQConnectionFactory setUser(String user) {
      checkWrite();
      this.user = user;
      return this;
   }

   public String getUser() {
      return user;
   }

   public String getPassword() {
      return password;
   }

   public ActiveMQConnectionFactory setPassword(String password) {
      checkWrite();
      this.password = password;
      return this;
   }

   public String getPasswordCodec() {
      return serverLocator.getPasswordCodec();
   }

   public ActiveMQConnectionFactory setPasswordCodec(String passwordCodec) {
      checkWrite();
      serverLocator.setPasswordCodec(passwordCodec);
      return this;
   }

   public void setGroupID(final String groupID) {
      serverLocator.setGroupID(groupID);
   }

   public String getGroupID() {
      return serverLocator.getGroupID();
   }

   public boolean isCompressLargeMessage() {
      return serverLocator.isCompressLargeMessage();
   }

   public void setCompressLargeMessage(boolean avoidLargeMessages) {
      serverLocator.setCompressLargeMessage(avoidLargeMessages);
   }

   public int getCompressionLevel() {
      return serverLocator.getCompressionLevel();
   }

   public void setCompressionLevel(int compressionLevel) {
      serverLocator.setCompressionLevel(compressionLevel);
   }

   @Override
   public void close() {
      ServerLocator locator0 = serverLocator;
      if (locator0 != null)
         locator0.close();
   }

   public ServerLocator getServerLocator() {
      return serverLocator;
   }

   public int getFactoryType() {
      return JMSFactoryType.CF.intValue();
   }

   protected ActiveMQConnection createConnectionInternal(final String username,
                                                                      final String password,
                                                                      final boolean isXA,
                                                                      final int type) throws JMSException {
      lock.lock();

      try {
         makeReadOnly();

         ClientSessionFactory factory;

         try {
            factory = serverLocator.createSessionFactory();
         } catch (Exception e) {
            JMSException jmse = new JMSException("Failed to create session factory");

            jmse.initCause(e);
            jmse.setLinkedException(e);

            throw jmse;
         }

         ActiveMQConnection connection = null;

         if (isXA) {
            if (type == ActiveMQConnection.TYPE_GENERIC_CONNECTION) {
               connection = new ActiveMQXAConnection(this, username, password, type, clientID, dupsOKBatchSize, transactionBatchSize, cacheDestinations, enable1xPrefixes, factory);
            } else if (type == ActiveMQConnection.TYPE_QUEUE_CONNECTION) {
               connection = new ActiveMQXAConnection(this, username, password, type, clientID, dupsOKBatchSize, transactionBatchSize, cacheDestinations, enable1xPrefixes, factory);
            } else if (type == ActiveMQConnection.TYPE_TOPIC_CONNECTION) {
               connection = new ActiveMQXAConnection(this, username, password, type, clientID, dupsOKBatchSize, transactionBatchSize, cacheDestinations, enable1xPrefixes, factory);
            }
         } else {
            if (type == ActiveMQConnection.TYPE_GENERIC_CONNECTION) {
               connection = new ActiveMQConnection(this, username, password, type, clientID, dupsOKBatchSize, transactionBatchSize, cacheDestinations, enable1xPrefixes, factory);
            } else if (type == ActiveMQConnection.TYPE_QUEUE_CONNECTION) {
               connection = new ActiveMQConnection(this, username, password, type, clientID, dupsOKBatchSize, transactionBatchSize, cacheDestinations, enable1xPrefixes, factory);
            } else if (type == ActiveMQConnection.TYPE_TOPIC_CONNECTION) {
               connection = new ActiveMQConnection(this, username, password, type, clientID, dupsOKBatchSize, transactionBatchSize, cacheDestinations, enable1xPrefixes, factory);
            }
         }

         if (connection == null) {
            throw new JMSException("Failed to create connection: invalid type " + type);
         }
         connection.setReference(this);

         try {
            connection.authorize(!isEnableSharedClientID());
         } catch (JMSException e) {
            try {
               connection.close();
            } catch (JMSException me) {
            }
            throw e;
         }

         return connection;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public String toString() {
      return "ActiveMQConnectionFactory [serverLocator=" + serverLocator +
         ", clientID=" +
         clientID +
         ", consumerWindowSize=" +
         getConsumerWindowSize() +
         ", dupsOKBatchSize=" +
         dupsOKBatchSize +
         ", transactionBatchSize=" +
         transactionBatchSize +
         ", readOnly=" +
         readOnly +
         ", EnableSharedClientID=" +
         enableSharedClientID +
         "]";
   }


   private void checkWrite() {
      if (readOnly) {
         throw new IllegalStateException("Cannot set attribute on ActiveMQConnectionFactory after it has been used");
      }
   }

   // this may need to be set by classes which extend this class
   protected void makeReadOnly() {
      this.readOnly = true;
   }
}
