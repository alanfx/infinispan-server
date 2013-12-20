package org.infinispan.server.test.client.hotrod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.jni.Flag;
import org.infinispan.client.hotrod.jni.Hotrod;
import org.infinispan.client.hotrod.jni.JniHelper;
import org.infinispan.client.hotrod.jni.MapReturn;
import org.infinispan.client.hotrod.jni.MetadataPairReturn;
import org.infinispan.client.hotrod.jni.RelayBytes;
import org.infinispan.client.hotrod.jni.RemoteCache_jb_jb;
import org.infinispan.client.hotrod.jni.VectorReturn;
import org.infinispan.client.hotrod.jni.VersionPairReturn;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.server.test.category.HotRodLocal;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests C++ and Java Hot Rod Client interoperability.
 * 
 * @author Alan Field
 */
@RunWith(Arquillian.class)
@Category({ HotRodLocal.class })
public class CrossLanguageHotRodTest {
   final String DEFAULT_CACHE_MANAGER = "local";
   final String DEFAULT_CACHE = "testcache";

   private static final Log log = LogFactory.getLog(CrossLanguageHotRodTest.class);

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @InfinispanResource("container2")
   RemoteInfinispanServer server2; //when run in LOCAL mode - inject here the same container as container1

   RemoteCacheManager javaRemoteCacheManager;
   org.infinispan.client.hotrod.jni.RemoteCacheManager cppRemoteCacheManager;
   RemoteCache_jb_jb cppCache;
   RemoteCache<String, Object> javaCache;
   Marshaller marshaller = new org.infinispan.commons.marshall.jboss.GenericJBossMarshaller();

   //Test data
   String v01 = "v0";
   String v02 = "ÅÄÇÉÑÖÕÜàäâáãçëèêéîïìíñôöòóüûùúÿ";
   String v03 = null;

   byte[] v11 = { 'v', 'a', 'l', 'u', 'e', '1' };
   boolean[] v12 = { true, false, false, true };
   char[] v13 = { 'v', 'à', 'l', 'û', 'è', '1' };
   double[] v14 = { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.MAX_VALUE, Double.MIN_VALUE,
         Double.MIN_NORMAL, Double.NaN, 0 };
   float[] v15 = { Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.MAX_VALUE, Float.MIN_VALUE,
         Float.MIN_NORMAL, Float.NaN, 0 };
   int[] v16 = { Integer.MAX_VALUE, Integer.MIN_VALUE, 0 };
   long[] v17 = { Long.MAX_VALUE, Long.MIN_VALUE, 0 };
   short[] v18 = { Short.MAX_VALUE, Short.MIN_VALUE, 0 };
   String[] v19 = { "ÅÄ", "Ç", "É", "Ñ", "ÖÕ", "Ü", "àäâáã", "ç", "ëèêé", "îïìí", "ñ", "ôöòó", "üûùú", "ÿ", null };

   boolean v21 = true;
   boolean v22 = false;

   byte v31 = 127;
   byte v32 = -128;
   byte v33 = 0;

   char v41 = '4';
   char v42 = 'Ç';

   double v51 = Double.NEGATIVE_INFINITY;
   double v52 = Double.POSITIVE_INFINITY;
   double v53 = Double.MAX_VALUE;
   double v54 = Double.MIN_VALUE;
   double v55 = Double.MIN_NORMAL;
   double v56 = Double.NaN;
   double v57 = 0;

   float v61 = Float.NEGATIVE_INFINITY;
   float v62 = Float.POSITIVE_INFINITY;
   float v63 = Float.MAX_VALUE;
   float v64 = Float.MIN_VALUE;
   float v65 = Float.MIN_NORMAL;
   float v66 = Float.NaN;
   float v67 = 0;

   int v71 = Integer.MIN_VALUE;
   int v72 = Integer.MAX_VALUE;
   int v73 = 0;

   long v81 = Long.MAX_VALUE;
   long v82 = Long.MIN_VALUE;
   long v83 = 0;

   short v91 = Short.MIN_VALUE;
   short v92 = Short.MAX_VALUE;
   short v93 = 0;

   Object v10 = null;

   Object[] valueArray = { v01, v02, v03, v11, v12, v13, v14, v15, v16, v17, v18, v19, v21, v22, v31, v32, v33, v41,
         v42, v51, v52, v53, v54, v55, v56, v57, v61, v62, v63, v64, v65, v66, v67, v71, v72, v73, v81, v82, v83, v91,
         v92, v93, v10 };

   String serverConfigPath = System.getProperty("server1.dist") + File.separator + "standalone" + File.separator
         + "configuration";

   String[] lifespanMaxIdleCommands = { "put", "putIfAbsent", "replace", "replaceWithVersion" };

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
   public void testDefaultCache() throws Exception {
      log.info("testDefaultCache()");
      doCppGetDefaultCache(DEFAULT_CACHE);
      doCppPut(DEFAULT_CACHE);
      doCppGet(DEFAULT_CACHE);
      doCppGetBulk(DEFAULT_CACHE);
      doCppRemove(DEFAULT_CACHE);
      doCppContainsKey(DEFAULT_CACHE);
      doCppReplace(DEFAULT_CACHE);
      doCppPutIfAbsent(DEFAULT_CACHE);

      doCppLifespan(DEFAULT_CACHE);
      doCppMaxIdle(DEFAULT_CACHE);
      doCppLifespanAndMaxIdle(DEFAULT_CACHE);
      doCppStats(DEFAULT_CACHE);
      doCppReplaceWithVersion(DEFAULT_CACHE);
      doCppRemoveWithVersion(DEFAULT_CACHE);
      doCppGetWithMetadata(DEFAULT_CACHE);
      log.info("All default cache tests PASSED");
   }
   
   /*
    * testDefaultCache methods
    */

   private void doCppGetDefaultCache(String cacheName) {
      log.info("doCppGetDefaultCache(String cacheName)");
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);
      RemoteCache_jb_jb cppCache2 = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, "", false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      assertTrue(cppCache2.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache2));

      for (int i = 0; i < valueArray.length; i++) {
         assertEquals(null, cppPut(cppCache, "k" + i, valueArray[i], true, -1, -1));
         assertEquals(i + 1, numEntries(cacheName, cppCache));
         assertEquals(i + 1, numEntries(cacheName, cppCache2));
      }

      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      assertEquals(valueArray.length, numEntries("", cppCache2));
      for (int i = 0; i < valueArray.length; i++) {
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + cppGet(cppCache, "k" + i) + ")",
               testEquality(valueArray[i], cppGet(cppCache, "k" + i)));
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + cppGet(cppCache2, "k" + i) + ")",
               testEquality(valueArray[i], cppGet(cppCache2, "k" + i)));
      }

      for (int i = 0; i < valueArray.length; i++) {
         if (i % 2 == 0) {
            Object removeResult = cppRemove(cppCache2, "k" + i, true);
            assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + removeResult + ")",
                  testEquality(valueArray[i], removeResult));
         } else {
            assertEquals(null, cppRemove(cppCache2, "k" + i, false));
         }
         assertEquals(valueArray.length - (i + 1), numEntries(cacheName, cppCache));
         assertEquals(valueArray.length - (i + 1), numEntries("", cppCache2));
      }

      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      assertTrue(cppCache2.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache2));
   }

   private void doCppPut(String cacheName) {
      log.info("doCppPut(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         assertEquals(null, cppPut(cppCache, "k" + i, valueArray[i], true, -1, -1));
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }

      assertEquals(valueArray.length, numEntries(cacheName, cppCache));

      for (int i = 0; i < valueArray.length; i++) {
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + javaCache.get("k" + i) + ")",
               testEquality(valueArray[i], javaCache.get("k" + i)));
      }

      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppReplace(String cacheName) {
      log.info("doCppReplace(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }

      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         if (i % 2 == 0) {
            Object replaceResult = cppReplace(cppCache, "k" + i, "v" + (i * 10));
            assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + replaceResult + ")",
                  testEquality(valueArray[i], replaceResult));
         } else {
            assertEquals(null, cppReplace(cppCache, "k" + i, "v" + (i * 10), false, -1, -1));
         }
      }
      for (int i = 0; i < valueArray.length; i++) {
         assertEquals("v" + (i * 10), javaCache.get("k" + i));
      }
      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppGet(String cacheName) {
      log.info("doCppGet(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }
      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + cppGet(cppCache, "k" + i) + ")",
               testEquality(valueArray[i], cppGet(cppCache, "k" + i)));
      }
      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppGetBulk(String cacheName) {
      log.info("doCppGetBulk(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }
      assertEquals(valueArray.length, numEntries(cacheName, cppCache));

      /*
       * getBulk(valueArray.length)
       */
      Map<Object, Object> result = cppGetBulk(cppCache, valueArray.length);
      assertEquals(valueArray.length, result.size());
      for (int i = 0; i < valueArray.length; i++) {
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + result.get("k" + i) + ")",
               testEquality(valueArray[i], result.get("k" + i)));
      }

      /*
       * getBulk(0)
       */
      result = cppGetBulk(cppCache, 0);
      assertEquals(valueArray.length, result.size());
      for (int i = 0; i < valueArray.length; i++) {
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + result.get("k" + i) + ")",
               testEquality(valueArray[i], result.get("k" + i)));
      }

      /*
       * getBulk(valueArray.length - 1)
       */
      result = cppGetBulk(cppCache, valueArray.length - 1);
      assertEquals(valueArray.length - 1, result.size());
      for (int i = 0; i < valueArray.length; i++) {
         if (result.containsKey("k" + i)) {
            assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + result.get("k" + i) + ")",
                  testEquality(valueArray[i], result.get("k" + i)));
         }
      }

      /*
       * getBulk(1)
       */
      result = cppGetBulk(cppCache, 1);
      assertEquals(1, result.size());
      for (int i = 0; i < valueArray.length; i++) {
         if (result.containsKey("k" + i)) {
            assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + result.get("k" + i) + ")",
                  testEquality(valueArray[i], result.get("k" + i)));
         }
      }

      /*
       * getBulk(valueArray.length + 1)
       */
      result = cppGetBulk(cppCache, valueArray.length + 1);
      assertEquals(valueArray.length, result.size());
      for (int i = 0; i < valueArray.length; i++) {
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + result.get("k" + i) + ")",
               testEquality(valueArray[i], result.get("k" + i)));
      }

      /*
       * getBulk(-1)
       */
      result = cppGetBulk(cppCache, -1);
      assertEquals(0, result.size());

      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppRemove(String cacheName) {
      log.info("doCppRemove(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }
      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         if (i % 2 == 0) {
            Object removeResult = cppRemove(cppCache, "k" + i, true);
            assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + removeResult + ")",
                  testEquality(valueArray[i], removeResult));
         } else {
            assertEquals(null, cppRemove(cppCache, "k" + i, false));
         }
         assertEquals(valueArray.length - (i + 1), numEntries(cacheName, cppCache));
      }
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppContainsKey(String cacheName) {
      log.info("doCppContainsKey(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }
      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         assertEquals(true, cppContainsKey(cppCache, "k" + i));
      }
      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppPutIfAbsent(String cacheName) {
      log.info("doCppPutIfAbsent(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         assertFalse(cppContainsKey(cppCache, "k" + i));
         assertEquals(null, cppPutIfAbsent(cppCache, "k" + i, valueArray[i], true, -1, -1));
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }
      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         assertTrue(cppContainsKey(cppCache, "k" + i));
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + javaCache.get("k" + i) + ")",
               testEquality(valueArray[i], javaCache.get("k" + i)));
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + cppGet(cppCache, "k" + i) + ")",
               testEquality(valueArray[i], cppGet(cppCache, "k" + i)));

         if (i % 2 == 0) {
            assertTrue(testEquality(valueArray[i], cppPutIfAbsent(cppCache, "k" + i, "newValue", true, -1, -1)));
         } else {
            assertEquals(null, cppPutIfAbsent(cppCache, "k" + i, "newValue", false, -1, -1));
         }
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + javaCache.get("k" + i) + ")",
               testEquality(valueArray[i], javaCache.get("k" + i)));
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + cppGet(cppCache, "k" + i) + ")",
               testEquality(valueArray[i], cppGet(cppCache, "k" + i)));
      }
      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppLifespan(String cacheName) {
      log.info("doCppLifespan(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);
      long lifespanSec = 2;

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (String command : lifespanMaxIdleCommands) {
         if (command.equals(lifespanMaxIdleCommands[0])) {
            for (int i = 0; i < valueArray.length; i++) {
               assertEquals(null, cppPut(cppCache, "k" + i, valueArray[i], false, lifespanSec, -1));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
            }
         } else if (command.equals(lifespanMaxIdleCommands[1])) {
            for (int i = 0; i < valueArray.length; i++) {
               assertEquals(null, cppPutIfAbsent(cppCache, "k" + i, valueArray[i], false, lifespanSec, -1));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
            }
         } else if (command.equals(lifespanMaxIdleCommands[2])) {
            for (int i = 0; i < valueArray.length; i++) {
               javaCache.put("k" + i, valueArray[i]);
               assertEquals(null, cppReplace(cppCache, "k" + i, valueArray[i], false, lifespanSec, -1));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
            }
         } else if (command.equals(lifespanMaxIdleCommands[3])) {
            for (int i = 0; i < valueArray.length; i++) {
               javaCache.put("k" + i, "javaData");
               long currentVersion = cppGetWithVersion(cppCache, "k" + i).getSecond().getVersion();

               assertTrue(cppReplaceWithVersion(cppCache, "k" + i, currentVersion, valueArray[i], lifespanSec, -1));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
            }
         }

         try {
            Thread.sleep(lifespanSec * 2000);
         } catch (InterruptedException e) {
            //Eat this!
         }

         for (int i = 0; i < valueArray.length; i++) {
            assertEquals(null, javaCache.get("k" + i));
            assertFalse(javaCache.containsKey("k" + i));
         }
         assertTrue(cppCache.isEmpty());
         assertEquals(0, numEntries(cacheName, cppCache));
      }
   }

   private void doCppMaxIdle(String cacheName) {
      log.info("doCppMaxIdle(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);
      long maxIdleSec = 2;

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (String command : lifespanMaxIdleCommands) {
         if (command.equals(lifespanMaxIdleCommands[0])) {
            for (int i = 0; i < valueArray.length; i++) {
               assertEquals(null, cppPut(cppCache, "k" + i, valueArray[i], false, -1, maxIdleSec));
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         } else if (command.equals(lifespanMaxIdleCommands[1])) {
            for (int i = 0; i < valueArray.length; i++) {
               assertEquals(null, cppPutIfAbsent(cppCache, "k" + i, valueArray[i], false, -1, maxIdleSec));
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         } else if (command.equals(lifespanMaxIdleCommands[2])) {
            for (int i = 0; i < valueArray.length; i++) {
               javaCache.put("k" + i, valueArray[i]);
               assertEquals(null, cppReplace(cppCache, "k" + i, valueArray[i], false, -1, maxIdleSec));
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         } else if (command.equals(lifespanMaxIdleCommands[3])) {
            for (int i = 0; i < valueArray.length; i++) {
               javaCache.put("k" + i, "javaData");
               long currentVersion = cppGetWithVersion(cppCache, "k" + i).getSecond().getVersion();

               assertTrue(cppReplaceWithVersion(cppCache, "k" + i, currentVersion, valueArray[i], -1, maxIdleSec));
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         }

         try {
            Thread.sleep(maxIdleSec * 2000);
         } catch (InterruptedException e) {
            //Eat this!
         }

         for (int i = 0; i < valueArray.length; i++) {
            assertEquals(null, javaCache.get("k" + i));
            assertFalse(javaCache.containsKey("k" + i));
         }
         assertTrue(cppCache.isEmpty());
         assertEquals(0, numEntries(cacheName, cppCache));
      }
   }

   private void doCppLifespanAndMaxIdle(String cacheName) {
      log.info("doCppLifespanAndMaxIdle(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);
      long lifespanSec = 2;
      long maxIdleSec = lifespanSec / 2;

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));

      for (String command : lifespanMaxIdleCommands) {
         if (command.equals(lifespanMaxIdleCommands[0])) {
            for (int i = 0; i < valueArray.length; i++) {
               assertEquals(null, cppPut(cppCache, "k" + i, valueArray[i], false, lifespanSec, maxIdleSec));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         } else if (command.equals(lifespanMaxIdleCommands[1])) {
            for (int i = 0; i < valueArray.length; i++) {
               assertEquals(null, cppPutIfAbsent(cppCache, "k" + i, valueArray[i], false, lifespanSec, maxIdleSec));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         } else if (command.equals(lifespanMaxIdleCommands[2])) {
            for (int i = 0; i < valueArray.length; i++) {
               javaCache.put("k" + i, "javaData");
               assertEquals(null, cppReplace(cppCache, "k" + i, valueArray[i], false, lifespanSec, maxIdleSec));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         } else if (command.equals(lifespanMaxIdleCommands[3])) {
            for (int i = 0; i < valueArray.length; i++) {
               javaCache.put("k" + i, "javaData");
               long currentVersion = cppGetWithVersion(cppCache, "k" + i).getSecond().getVersion();

               assertTrue(cppReplaceWithVersion(cppCache, "k" + i, currentVersion, valueArray[i], lifespanSec,
                     maxIdleSec));
               assertEquals(lifespanSec, javaCache.getWithMetadata("k" + i).getLifespan());
               assertEquals(lifespanSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getLifespan());
               assertEquals(maxIdleSec, javaCache.getWithMetadata("k" + i).getMaxIdle());
               assertEquals(maxIdleSec, this.cppGetWithMetadata(cppCache, "k" + i).getSecond().getMaxIdle());
            }
         }

         try {
            Thread.sleep(maxIdleSec * 500);
         } catch (InterruptedException e) {
            //Eat this!
         }
         for (int i = 0; i < valueArray.length; i++) {
            assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + javaCache.get("k" + i) + ")",
                  testEquality(valueArray[i], javaCache.get("k" + i)));
         }

         try {
            Thread.sleep(lifespanSec * 2000);
         } catch (InterruptedException e) {
            //Eat this!
         }
         for (int i = 0; i < valueArray.length; i++) {
            assertEquals(null, javaCache.get("k" + i));
            assertFalse(javaCache.containsKey("k" + i));
         }
         assertTrue(cppCache.isEmpty());
         assertEquals(0, numEntries(cacheName, cppCache));
      }
   }

   private void doCppStats(String cacheName) {
      log.info("doCppStats(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      for (String key : javaCache.stats().getStatsMap().keySet()) {
         log.debug("Key: " + key + "; Value: " + cppGetLongCacheStat(cppCache, key));
      }
      assertEquals(0, cppGetLongCacheStat(cppCache, "currentNumberOfEntries"));

      long totalNumberOfEntries = cppGetLongCacheStat(cppCache, "totalNumberOfEntries");
      long stores = cppGetLongCacheStat(cppCache, "stores");

      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, cppGetLongCacheStat(cppCache, "currentNumberOfEntries"));
         assertEquals(totalNumberOfEntries + i + 1, cppGetLongCacheStat(cppCache, "totalNumberOfEntries"));
         assertEquals(stores + i + 1, cppGetLongCacheStat(cppCache, "stores"));
      }

      //Store initial values
      assertEquals(numEntries(cacheName, cppCache), cppGetLongCacheStat(cppCache, "currentNumberOfEntries"));
      long hits = cppGetLongCacheStat(cppCache, "hits");
      long removeMisses = cppGetLongCacheStat(cppCache, "removeMisses");
      long removeHits = cppGetLongCacheStat(cppCache, "removeHits");
      long retrievals = cppGetLongCacheStat(cppCache, "retrievals");
      long misses = cppGetLongCacheStat(cppCache, "misses");

      //hit
      assertTrue(testEquality(valueArray[0], cppGet(cppCache, "k0")));
      assertEquals(hits + 1, cppGetLongCacheStat(cppCache, "hits"));
      assertEquals(retrievals + 1, cppGetLongCacheStat(cppCache, "retrievals"));
      //miss
      assertTrue(testEquality(null, cppGet(cppCache, "NON_EXISTENT")));
      assertEquals(misses + 1, cppGetLongCacheStat(cppCache, "misses"));
      assertEquals(retrievals + 2, cppGetLongCacheStat(cppCache, "retrievals"));
      //removeHits
      assertEquals(null, cppRemove(cppCache, "k0", false));
      assertEquals(removeHits + 1, cppGetLongCacheStat(cppCache, "removeHits"));
      assertEquals(numEntries(cacheName, cppCache), cppGetLongCacheStat(cppCache, "currentNumberOfEntries"));
      //removeMisses
      assertEquals(null, cppRemove(cppCache, "NON_EXISTENT", false));
      assertEquals(removeMisses + 1, cppGetLongCacheStat(cppCache, "removeMisses"));

      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppReplaceWithVersion(String cacheName) {
      log.info("doCppReplaceWithVersion(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }

      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         long currentVersion = cppGetWithVersion(cppCache, "k" + i).getSecond().getVersion();

         assertFalse(cppReplaceWithVersion(cppCache, "k" + i, currentVersion + 1, "replacedValue", -1, -1));
         assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + javaCache.get("k" + i) + ")",
               testEquality(valueArray[i], javaCache.get("k" + i)));
         assertTrue(cppReplaceWithVersion(cppCache, "k" + i, currentVersion, "replacedValue", -1, -1));
         assertTrue("Expected: (replacedValue), Actual: (" + javaCache.get("k" + i) + ")",
               testEquality("replacedValue", javaCache.get("k" + i)));
      }
      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppRemoveWithVersion(String cacheName) {
      log.info("doCppRemoveWithVersion(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }
      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         long currentVersion = cppGetWithVersion(cppCache, "k" + i).getSecond().getVersion();

         assertFalse(cppRemoveWithVersion(cppCache, "k" + i, currentVersion + 1));
         assertTrue(cppContainsKey(cppCache, "k" + i));
         assertTrue(cppRemoveWithVersion(cppCache, "k" + i, currentVersion));
         assertFalse(cppContainsKey(cppCache, "k" + i));
         assertEquals(valueArray.length - (i + 1), numEntries(cacheName, cppCache));
      }
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   private void doCppGetWithMetadata(String cacheName) {
      log.info("doCppGetWithMetadata(String cacheName)");
      javaCache = javaRemoteCacheManager.getCache(cacheName);
      cppCache = Hotrod.getJniRelayNamedCache(cppRemoteCacheManager, cacheName, false);

      assertTrue(cppRemoteCacheManager.isStarted());
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         javaCache.put("k" + i, valueArray[i]);
         assertEquals(i + 1, numEntries(cacheName, cppCache));
      }
      assertEquals(valueArray.length, numEntries(cacheName, cppCache));
      for (int i = 0; i < valueArray.length; i++) {
         MetadataPairReturn result = cppGetWithMetadata(cppCache, "k" + i);
         MetadataValue<Object> javaMeta = javaCache.getWithMetadata("k" + i);
         // From Java: MetadataValueImpl [created=-1, lifespan=-1, lastUsed=-1, maxIdle=-1, getVersion()=1334, getValue()=v0]

         if (result.getFirst() != null) {
            RelayBytes vrb = null;
            try {
               vrb = Hotrod.dereference(result.getFirst());
               if (vrb != null) {
                  Object value = unmarshall(vrb);
                  org.infinispan.client.hotrod.jni.MetadataValue cppMeta = result.getSecond();
                  assertTrue("Expected: (" + valueArray[i] + "), Actual: (" + value + ")",
                        testEquality(valueArray[i], value));
                  // TODO Uncomment when HRCPP-63 is fixed
//                  assertEquals(javaMeta.getCreated(), cppMeta.getCreated());
//                  assertEquals(javaMeta.getLastUsed(), cppMeta.getLastUsed());
                  assertEquals(javaMeta.getLifespan(), cppMeta.getLifespan());
                  assertEquals(javaMeta.getMaxIdle(), cppMeta.getMaxIdle());
                  assertEquals(javaMeta.getVersion(), cppMeta.getVersion());
               }
            } finally {
               JniHelper.dispose(vrb);
            }
         }

      }
      cppClear(cppCache, cacheName);
      assertTrue(cppCache.isEmpty());
      assertEquals(0, numEntries(cacheName, cppCache));
   }

   /*
    * RemoteCache_jb_jb helper methods
    */
   private boolean isLocalMode() {
      return System.getProperty("clustering.mode", "dist").contains("local");
   }

   private long numEntries(String cacheName, RemoteCache_jb_jb cppCache) {
      long entryCount = -1;
      // TODO verify that RemoteCache.size() is only valid in local mode
      if (isLocalMode()) {
         entryCount = cppCache.size().longValue();
      } else {
         entryCount = server1.getCacheManager(DEFAULT_CACHE_MANAGER).getCache(cacheName).getNumberOfEntries();
      }
      return entryCount;
   }

   private long cppGetLongCacheStat(RemoteCache_jb_jb cppCache, String statName) {
      return Long.parseLong(cppCache.stats().get(statName));
   }

   private Object cppPut(RemoteCache_jb_jb cache, Object key, Object value, boolean forceReturnValue, long lifespan,
         long maxIdle) {
      log.debug("cppPut(RemoteCache_jb_jb cache, Object key, Object value, boolean forceReturnValue)");
      Object result = null;
      RelayBytes krb = marshall(key);
      RelayBytes vrb = marshall(value);
      try {
         if (forceReturnValue) {
            cache = cache.withFlags(Flag.FORCE_RETURN_VALUE);
         }
         if (maxIdle > 0) {
            result = unmarshall(cache.put(krb, vrb, BigInteger.valueOf(lifespan), BigInteger.valueOf(maxIdle)));
         } else {
            if (lifespan > 0) {
               result = unmarshall(cache.put(krb, vrb, BigInteger.valueOf(lifespan)));
            } else {
               result = unmarshall(cache.put(krb, vrb));
            }
         }
      } finally {
         JniHelper.dispose(krb);
         JniHelper.dispose(vrb);
      }
      return result;
   }

   private Object cppGet(RemoteCache_jb_jb cache, Object key) {
      log.debug("cppGet(RemoteCache_jb_jb cache, Object key)");
      RelayBytes krb = marshall(key);
      Object result = null;
      try {
         result = unmarshall(cache.get(krb));
      } finally {
         JniHelper.dispose(krb);
      }
      return result;
   }

   private Map<Object, Object> cppGetBulk(RemoteCache_jb_jb cache, int nrOfEntries) {
      MapReturn mapReturn = cache.getBulk(nrOfEntries);
      VectorReturn vectorReturn = Hotrod.keySet(mapReturn);

      Map<Object, Object> result = new HashMap<Object, Object>();
      RelayBytes krb = null;
      RelayBytes vrb = null;
      for (int i = 0; i < vectorReturn.size(); i++) {
         try {
            krb = Hotrod.dereference(vectorReturn.get(i));
            vrb = Hotrod.dereference(mapReturn.get(vectorReturn.get(i)));
            result.put(unmarshall(krb), unmarshall(vrb));
         } finally {
            JniHelper.dispose(krb);
            JniHelper.dispose(vrb);
         }
      }

      return result;
   }

   private MetadataPairReturn cppGetWithMetadata(RemoteCache_jb_jb cache, Object key) {
      RelayBytes krb = marshall(key);
      MetadataPairReturn result = null;
      try {
         result = cache.getWithMetadata(krb);
      } finally {
         JniHelper.dispose(krb);
      }
      return result;
   }

   private VersionPairReturn cppGetWithVersion(RemoteCache_jb_jb cache, Object key) {
      RelayBytes krb = marshall(key);
      VersionPairReturn result = null;
      try {
         result = cache.getWithVersion(krb);
      } finally {
         JniHelper.dispose(krb);
      }
      return result;
   }

   private Object cppRemove(RemoteCache_jb_jb cache, Object key, boolean forceReturnValue) {
      log.debug("cppRemove(RemoteCache_jb_jb cache, Object key, boolean forceReturnValue)");
      RelayBytes krb = marshall(key);
      Object result = null;
      try {
         if (forceReturnValue) {
            result = unmarshall(cache.withFlags(Flag.FORCE_RETURN_VALUE).remove(krb));
         } else {
            result = unmarshall(cache.remove(krb));
         }
      } finally {
         JniHelper.dispose(krb);
      }
      return result;
   }

   private boolean cppRemoveWithVersion(RemoteCache_jb_jb cache, Object key, long version) {
      log.debug("cppRemoveWithVersion(RemoteCache_jb_jb cache, Object key, long version)");
      RelayBytes krb = marshall(key);
      boolean result = false;
      try {
         result = cache.removeWithVersion(krb, BigInteger.valueOf(version));
      } finally {
         JniHelper.dispose(krb);
      }
      return result;
   }

   private boolean cppContainsKey(RemoteCache_jb_jb cache, Object key) {
      log.debug("cppContainsKey(RemoteCache_jb_jb cache, Object key");
      RelayBytes krb = marshall(key);
      try {
         return cache.containsKey(krb);
      } finally {
         JniHelper.dispose(krb);
      }
   }

   private Object cppReplace(RemoteCache_jb_jb cache, Object key, Object value) {
      log.debug("cppReplace(RemoteCache_jb_jb cache, Object key, Object value)");
      return cppReplace(cache, key, value, true, -1, -1);
   }

   private Object cppReplace(RemoteCache_jb_jb cache, Object key, Object value, boolean forceReturnValue,
         long lifespan, long maxIdle) {
      log.debug("cppReplace(RemoteCache_jb_jb cache, Object key, Object value, boolean forceReturnValue)");
      Object result = null;
      RelayBytes krb = marshall(key);
      RelayBytes vrb = marshall(value);
      try {
         if (forceReturnValue) {
            cache = cache.withFlags(Flag.FORCE_RETURN_VALUE);
         }
         if (maxIdle > 0) {
            result = unmarshall(cache.replace(krb, vrb, BigInteger.valueOf(lifespan), BigInteger.valueOf(maxIdle)));
         } else {
            if (lifespan > 0) {
               result = unmarshall(cache.replace(krb, vrb, BigInteger.valueOf(lifespan)));
            } else {
               result = unmarshall(cache.replace(krb, vrb));
            }
         }
      } finally {
         JniHelper.dispose(krb);
         JniHelper.dispose(vrb);
      }
      return result;
   }

   //   private long cppGetVersioned(RemoteCache_jb_jb cache, Object key){
   //      RelayBytes krb = marshall(key);
   //      try {
   //         long rb = HotrodJNI.new_RelayBytes();
   //         SWIGTYPE_p_std__pairT_HR_SHARED_PTRT_RelayBytes_t_infinispan__hotrod__VersionedValue_t ptr = cache.getWithVersion(krb);
   //         long cPtr = SWIGTYPE_p_std__pairT_HR_SHARED_PTRT_RelayBytes_t_infinispan__hotrod__VersionedValue_t.
   //         if(ptr == 0){
   //            null}
   //         else{
   //            new RelayBytes(ptr, false);
   //            
   //         }
   //      } finally {
   //         JniHelper.dispose(krb);
   //      }
   //      return -1;
   //   }

   private boolean cppReplaceWithVersion(RemoteCache_jb_jb cache, Object key, long version, Object value,
         long lifespan, long maxIdle) {
      log.debug("cppReplaceWithVersion(RemoteCache_jb_jb cache, Object key, long version, Object value, long lifespan, long maxIdle)");
      boolean result = false;
      RelayBytes krb = marshall(key);
      RelayBytes vrb = marshall(value);
      try {
         if (maxIdle > 0) {
            result = cache.replaceWithVersion(krb, vrb, BigInteger.valueOf(version), BigInteger.valueOf(lifespan),
                  BigInteger.valueOf(maxIdle));
         } else {
            if (lifespan > 0) {
               result = cache.replaceWithVersion(krb, vrb, BigInteger.valueOf(version), BigInteger.valueOf(lifespan));
            } else {
               result = cache.replaceWithVersion(krb, vrb, BigInteger.valueOf(version));
            }
         }
      } finally {
         JniHelper.dispose(krb);
         JniHelper.dispose(vrb);
      }
      return result;
   }

   private Object cppPutIfAbsent(RemoteCache_jb_jb cache, Object key, Object value, boolean forceReturnValue,
         long lifespan, long maxIdle) {
      log.debug("cppPutIfAbsent(RemoteCache_jb_jb cache, Object key, Object value, boolean forceReturnValue)");
      Object result = null;
      RelayBytes krb = marshall(key);
      RelayBytes vrb = marshall(value);
      try {
         if (forceReturnValue) {
            cache = cache.withFlags(Flag.FORCE_RETURN_VALUE);
         }
         if (maxIdle > 0) {
            result = unmarshall(cache.putIfAbsent(krb, vrb, BigInteger.valueOf(lifespan), BigInteger.valueOf(maxIdle)));
         } else {
            if (lifespan > 0) {
               result = unmarshall(cache.putIfAbsent(krb, vrb, BigInteger.valueOf(lifespan)));
            } else {
               result = unmarshall(cache.putIfAbsent(krb, vrb));
            }
         }
      } finally {
         JniHelper.dispose(krb);
         JniHelper.dispose(vrb);
      }
      return result;
   }

   private void cppClear(RemoteCache_jb_jb cache, String cacheName) {
      log.debug("cppClear()");
      cache.clear();
   }

   /*
    * Marshalling utilities
    */

   private RelayBytes marshall(Object source) {
      log.debug("marshall(Object source)");
      RelayBytes result = new RelayBytes();
      try {
         byte[] sourceBytes = marshaller.objectToByteBuffer(source);
         JniHelper.setJvmBytes(result, sourceBytes);
      } catch (Exception e) {
         log.error("Marshall error");
         e.printStackTrace();
      }
      return result;
   }

   private Object unmarshall(RelayBytes nativeSource) {
      log.debug("unmarshall(RelayBytes nativeSource)");
      Object result = null;
      if (nativeSource != null) {
         byte[] jcopy = new byte[(int) nativeSource.getLength()];
         JniHelper.readNative(nativeSource, jcopy);
         try {
            result = marshaller.objectFromByteBuffer(jcopy);
         } catch (Exception e) {
            log.error("Unmarshall error");
            e.printStackTrace();
         }
      }
      JniHelper.dispose(nativeSource);
      return result;
   }

   /**
    * 
    * Utility method to test object equality including arrays
    * 
    * @param obj1
    *           first object
    * @param obj2
    *           second object
    * @return <code>true</code> if the objects are equal, else <code>false</code>
    */
   private boolean testEquality(Object obj1, Object obj2) {
      boolean result = false;
      if (obj1 != null && obj1.getClass().isArray()) {
         if (obj1 instanceof byte[]) {
            result = Arrays.equals((byte[]) obj1, (byte[]) obj2);
         } else if (obj1 instanceof boolean[]) {
            result = Arrays.equals((boolean[]) obj1, (boolean[]) obj2);
         } else if (obj1 instanceof char[]) {
            result = Arrays.equals((char[]) obj1, (char[]) obj2);
         } else if (obj1 instanceof double[]) {
            result = Arrays.equals((double[]) obj1, (double[]) obj2);
         } else if (obj1 instanceof float[]) {
            result = Arrays.equals((float[]) obj1, (float[]) obj2);
         } else if (obj1 instanceof int[]) {
            result = Arrays.equals((int[]) obj1, (int[]) obj2);
         } else if (obj1 instanceof long[]) {
            result = Arrays.equals((long[]) obj1, (long[]) obj2);
         } else if (obj1 instanceof short[]) {
            result = Arrays.equals((short[]) obj1, (short[]) obj2);
         } else if (obj1 instanceof Object[]) {
            result = Arrays.deepEquals((Object[]) obj1, (Object[]) obj2);
         }
      } else {
         result = (obj1 == obj2 || obj1.equals(obj2));
      }
      return result;
   }

   static {
      System.loadLibrary("hotrod-jni");
      System.out.println("Loaded hotrod-jni library");
   }
}