package org.infinispan.server.test.client.hotrod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.jni.Configuration;
import org.infinispan.client.hotrod.jni.Hotrod;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.server.test.category.HotRodLocal;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests C++ RemoteCacheManager functionality.
 * 
 * @author Alan Field
 */
@RunWith(Arquillian.class)
@Category({ HotRodLocal.class })
public class CppRemoteCacheManagerTest {

   private static final Log log = LogFactory.getLog(CppRemoteCacheManagerTest.class);

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @InfinispanResource("container2")
   RemoteInfinispanServer server2; //when run in LOCAL mode - inject here the same container as container1

   RemoteCacheManager javaRemoteCacheManager;
   org.infinispan.client.hotrod.jni.RemoteCacheManager cppRemoteCacheManager;

   String serverConfigPath = System.getProperty("server1.dist") + File.separator + "standalone" + File.separator
         + "configuration";

   @Before
   public void setUp() {
      log.info("setUp()");
      ConfigurationBuilder javaBuilder = new ConfigurationBuilder();
      javaBuilder.addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort());

      javaRemoteCacheManager = new RemoteCacheManager(javaBuilder.build());

      org.infinispan.client.hotrod.jni.ConfigurationBuilder cppBuilder = new org.infinispan.client.hotrod.jni.ConfigurationBuilder();
      cppBuilder.addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort());

      cppRemoteCacheManager = new org.infinispan.client.hotrod.jni.RemoteCacheManager(cppBuilder.build(), false);

      assertFalse(cppRemoteCacheManager.isStarted());
      cppRemoteCacheManager.start();
      assertTrue(cppRemoteCacheManager.isStarted());
   }

   @Test
   public void testRemoteCacheManager() throws Exception {
      //TODO Remove these RemoteCacheManager tests once JNI support is implemented

      //testStartStop
      log.info("testRemoteCacheManager()");
      testStartStop();

      testGetNonExistentCache();

      //Add tests for configuration
      testDefaultConfiguration();
      testCustomConfiguration();
      log.info("All RemoteCacheManager tests PASSED");
   }

   /*
    * testRemoteCacheManager methods
    */
   
   private void testStartStop() {
      log.info("testStartStop()");
      cppRemoteCacheManager.stop();
      assertFalse(cppRemoteCacheManager.isStarted());
      cppRemoteCacheManager.start();
      assertTrue(cppRemoteCacheManager.isStarted());
   }

   private void testGetNonExistentCache() {
      log.info("testGetNonExistentCache()");
      try {
         Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, "NON_EXISTENT", false);
         fail("Should throw CacheNotFoundException");
      } catch (Exception e) {
         //ok
      }
   }

   private void testDefaultConfiguration() {
      log.info("testDefaultConfiguration()");
      org.infinispan.client.hotrod.jni.ConfigurationBuilder cppBuilder = new org.infinispan.client.hotrod.jni.ConfigurationBuilder();
      cppBuilder.addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort());

      org.infinispan.client.hotrod.jni.Configuration config = cppBuilder.build();
      //TODO Uncomment lines below when the C++ client supports these methods
      org.infinispan.client.hotrod.jni.Configuration config2 = cppBuilder
      //                    .balancingStrategy("org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy")
            .forceReturnValues(false).tcpNoDelay(true).pingOnStartup(true)
            //                          .transportFactory("org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory")
            //                          .marshaller("org.infinispan.commons.marshall.jboss.GenericJBossMarshaller")
            //                          .asyncExecutorFactory().factoryClass("org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory")
            //                          .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.pool_size", "10")
            //                          .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.queue_size", "100000")
            .keySizeEstimate(64).valueSizeEstimate(512).build();

      if (!isLocalMode()) {
         //Not implemented yet
         //                  cppBuilder.consistentHashImpl(1, "org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1");
      }

      assertEquals(config.getConnectionTimeout(), config2.getConnectionTimeout());
      assertEquals(config.getKeySizeEstimate(), config2.getKeySizeEstimate());
      assertEquals(config.getProtocolVersion(), config2.getProtocolVersion());
      assertEquals(config.getSocketTimeout(), config2.getSocketTimeout());
      assertEquals(config.getValueSizeEstimate(), config2.getValueSizeEstimate());
      assertEquals(config.isForceReturnValue(), config2.isForceReturnValue());
      assertEquals(config.isPingOnStartup(), config2.isPingOnStartup());
      assertEquals(config.isTcpNoDelay(), config2.isTcpNoDelay());

      assertEquals(config.getConnectionPoolConfiguration().getExhaustedAction(), config2
            .getConnectionPoolConfiguration().getExhaustedAction());
      assertEquals(config.getConnectionPoolConfiguration().isLifo(), config2.getConnectionPoolConfiguration().isLifo());
      assertEquals(config.getConnectionPoolConfiguration().getMaxActive(), config2.getConnectionPoolConfiguration()
            .getMaxActive());
      assertEquals(config.getConnectionPoolConfiguration().getMaxTotal(), config2.getConnectionPoolConfiguration()
            .getMaxTotal());
      assertEquals(config.getConnectionPoolConfiguration().getMaxWait(), config2.getConnectionPoolConfiguration()
            .getMaxWait());
      assertEquals(config.getConnectionPoolConfiguration().getMaxIdle(), config2.getConnectionPoolConfiguration()
            .getMaxIdle());
      assertEquals(config.getConnectionPoolConfiguration().getMinIdle(), config2.getConnectionPoolConfiguration()
            .getMinIdle());
      assertEquals(config.getConnectionPoolConfiguration().getNumTestsPerEvictionRun(), config2
            .getConnectionPoolConfiguration().getNumTestsPerEvictionRun());
      assertEquals(config.getConnectionPoolConfiguration().getTimeBetweenEvictionRuns(), config2
            .getConnectionPoolConfiguration().getTimeBetweenEvictionRuns());
      assertEquals(config.getConnectionPoolConfiguration().getMinEvictableIdleTime(), config2
            .getConnectionPoolConfiguration().getMinEvictableIdleTime());
      assertEquals(config.getConnectionPoolConfiguration().isTestOnBorrow(), config2.getConnectionPoolConfiguration()
            .isTestOnBorrow());
      assertEquals(config.getConnectionPoolConfiguration().isTestOnReturn(), config2.getConnectionPoolConfiguration()
            .isTestOnReturn());
      assertEquals(config.getConnectionPoolConfiguration().isTestWhileIdle(), config2.getConnectionPoolConfiguration()
            .isTestWhileIdle());
   }

   private void testCustomConfiguration() {
      log.info("testCustomConfiguration()");
      int delta = 10;
      org.infinispan.client.hotrod.jni.ConfigurationBuilder cppBuilder = new org.infinispan.client.hotrod.jni.ConfigurationBuilder();
      cppBuilder.addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort());
      Configuration defaultConfig = cppBuilder.build();

      cppBuilder = cppBuilder.connectionTimeout(defaultConfig.getConnectionTimeout() + delta);
      cppBuilder = cppBuilder.forceReturnValues(!defaultConfig.isForceReturnValue());
      cppBuilder = cppBuilder.keySizeEstimate(defaultConfig.getKeySizeEstimate() + delta);
      cppBuilder = cppBuilder.pingOnStartup(!defaultConfig.isPingOnStartup());
      cppBuilder = cppBuilder.socketTimeout(defaultConfig.getSocketTimeout() + delta);
      cppBuilder = cppBuilder.tcpNoDelay(!defaultConfig.isTcpNoDelay());
      cppBuilder = cppBuilder.valueSizeEstimate(defaultConfig.getValueSizeEstimate() + delta);

      //      SslConfigurationBuilder sslConfig = cppBuilder.ssl();
      //      ServerConfigurationBuilder serverConfig = cppBuilder.addServer();
      //      serverConfig.host("host");
      //      serverConfig.port(0);

      cppBuilder
            .connectionPool()
            .lifo(!defaultConfig.getConnectionPoolConfiguration().isLifo())
            .maxActive(defaultConfig.getConnectionPoolConfiguration().getMaxActive() + delta)
            .maxIdle(defaultConfig.getConnectionPoolConfiguration().getMaxIdle() + delta)
            .maxTotal(defaultConfig.getConnectionPoolConfiguration().getMaxTotal() + delta)
            .maxWait(defaultConfig.getConnectionPoolConfiguration().getMaxWait() + delta)
            .minEvictableIdleTime(defaultConfig.getConnectionPoolConfiguration().getMinEvictableIdleTime() + delta)
            .minIdle(defaultConfig.getConnectionPoolConfiguration().getMinIdle() + delta)
            .numTestsPerEvictionRun(defaultConfig.getConnectionPoolConfiguration().getNumTestsPerEvictionRun() + delta)
            .testOnBorrow(!defaultConfig.getConnectionPoolConfiguration().isTestOnBorrow())
            .testOnReturn(!defaultConfig.getConnectionPoolConfiguration().isTestOnReturn())
            .testWhileIdle(!defaultConfig.getConnectionPoolConfiguration().isTestWhileIdle())
            .timeBetweenEvictionRuns(
                  defaultConfig.getConnectionPoolConfiguration().getTimeBetweenEvictionRuns() + delta);

      Configuration customConfig = cppBuilder.build();
      assertEquals(defaultConfig.getConnectionTimeout() + delta, customConfig.getConnectionTimeout());
      assertEquals(!defaultConfig.isForceReturnValue(), customConfig.isForceReturnValue());
      assertEquals(defaultConfig.getKeySizeEstimate() + delta, customConfig.getKeySizeEstimate());
      assertEquals(!defaultConfig.isPingOnStartup(), customConfig.isPingOnStartup());
      assertEquals(defaultConfig.getSocketTimeout() + delta, customConfig.getSocketTimeout());
      assertEquals(!defaultConfig.isTcpNoDelay(), customConfig.isTcpNoDelay());
      assertEquals(defaultConfig.getValueSizeEstimate() + delta, customConfig.getValueSizeEstimate());
      //      assertEquals(customConfig.getServersConfiguration(), customConfig.getServersConfiguration());

      assertEquals(!defaultConfig.getConnectionPoolConfiguration().isLifo(), customConfig
            .getConnectionPoolConfiguration().isLifo());
      assertEquals(defaultConfig.getConnectionPoolConfiguration().getMaxActive() + delta, customConfig
            .getConnectionPoolConfiguration().getMaxActive());
      assertEquals(defaultConfig.getConnectionPoolConfiguration().getMaxIdle() + delta, customConfig
            .getConnectionPoolConfiguration().getMaxIdle());
      assertEquals(defaultConfig.getConnectionPoolConfiguration().getMaxTotal() + delta, customConfig
            .getConnectionPoolConfiguration().getMaxTotal());
      assertEquals(defaultConfig.getConnectionPoolConfiguration().getMaxWait() + delta, customConfig
            .getConnectionPoolConfiguration().getMaxWait());
      assertEquals(defaultConfig.getConnectionPoolConfiguration().getMinEvictableIdleTime() + delta, customConfig
            .getConnectionPoolConfiguration().getMinEvictableIdleTime());
      assertEquals(!defaultConfig.getConnectionPoolConfiguration().isTestOnBorrow(), customConfig
            .getConnectionPoolConfiguration().isTestOnBorrow());
      assertEquals(!defaultConfig.getConnectionPoolConfiguration().isTestOnReturn(), customConfig
            .getConnectionPoolConfiguration().isTestOnReturn());
      assertEquals(!defaultConfig.getConnectionPoolConfiguration().isTestWhileIdle(), customConfig
            .getConnectionPoolConfiguration().isTestWhileIdle());
      assertEquals(defaultConfig.getConnectionPoolConfiguration().getTimeBetweenEvictionRuns() + delta, customConfig
            .getConnectionPoolConfiguration().getTimeBetweenEvictionRuns());

   }
   
   private boolean isLocalMode() {
      return System.getProperty("clustering.mode", "dist").contains("local");
   }

   static {
      System.loadLibrary("hotrod-jni");
      System.out.println("Loaded hotrod-jni library");
   }
}