 /*
  * Copyright © 2022 Yazaki Kako Corporation. All Rights Reserved
  */

 package com.example.demo.utils;

 import com.fasterxml.jackson.databind.ObjectMapper;
 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.Collection;
 import java.util.Collections;
 import java.util.List;
 import java.util.Map;
 import java.util.Set;
 import java.util.concurrent.TimeUnit;
 import java.util.function.BiFunction;
 import org.springframework.beans.factory.annotation.Autowired;
 import org.springframework.data.redis.connection.RedisConnection;
 import org.springframework.data.redis.connection.RedisConnectionFactory;
 import org.springframework.data.redis.core.Cursor;
 import org.springframework.data.redis.core.RedisConnectionUtils;
 import org.springframework.data.redis.core.RedisTemplate;
 import org.springframework.data.redis.core.ScanOptions;
 import org.springframework.data.redis.core.StringRedisTemplate;
 import org.springframework.stereotype.Component;
 import org.springframework.util.ObjectUtils;

 /**
  * @author IBM陳玉体
  * @version 0.0.1
  * @since 2023/9/15 10:04
  */
 @Component
 public class RedisUtils {

   /**
    * クラスター検索単位.
    */
   private static final long SCAN_COUNT_UNIT = 10000;

   /**
    * Redisオペレーター.
    */
   @Autowired
   private RedisTemplate<String, String> redisTemplate;

   /**
    * Redisオペレーター(String).
    */
   @Autowired
   private StringRedisTemplate stringRedisTemplate;


   /**
    * 曖昧検索.
    *
    * @param keyPattern     検索KEY（*,?,[]ができます）
    * @param filterFunction フィルター関数（戻り値がTrueの場合、最終戻り値に追加する
    * @return マップの検索結果.
    */
   public List<Map<String, String>> filter(String[] keyPattern,
                                           BiFunction<Map<String, String>, String, Boolean> filterFunction) {
     return filter(keyPattern, null, null, filterFunction);
   }

   /**
    * フェルトのない曖昧検索.
    *
    * @param keyPattern 検索KEY（*,?,[]ができます）
    * @return マップの検索結果.
    */
   public List<Map<String, String>> filter(String[] keyPattern) {
     return filter(keyPattern, null, null, null);
   }

   /**
    * フェルトのない曖昧検索.
    *
    * @param keyPattern 検索KEY（*,?,[]ができます）
    * @param mapper     データコンバート
    * @param entityType クラスタイプ
    * @param <T>        コンバートしたいクラスタイプ
    * @return Tタイプの検索結果.
    */
   public <T> List<T> filter(String[] keyPattern, ObjectMapper mapper, Class<T> entityType) {
     return filter(keyPattern, mapper, entityType, null);
   }

   /**
    * 曖昧検索.
    *
    * @param keyPattern     検索KEY（*,?,[]ができます）
    * @param mapper         データコンバート
    * @param entityType     クラスタイプ
    * @param filterFunction フィルター関数（戻り値がTrueの場合、最終戻り値に追加する
    * @param <T>            コンバートしたいクラスタイプ
    * @return Tタイプの検索結果.
    */
   public <T> List<T> filter(String[] keyPattern, ObjectMapper mapper, Class<T> entityType,
                             BiFunction<T, String, Boolean> filterFunction) {
     String cacheKeyPattern = connectKeys(keyPattern);
     List<T> result = new ArrayList<>();
     RedisConnectionFactory factory = redisTemplate.getConnectionFactory();
     if (factory == null) {
       return Collections.emptyList();
     }
     RedisConnection connection = factory.getConnection();
     try (Cursor<byte[]> cursor = connection.scan(
         ScanOptions.scanOptions().match(cacheKeyPattern).count(SCAN_COUNT_UNIT).build())) {
       while (cursor.hasNext()) {
         String key = new String(cursor.next());
         Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);
         T t;
         if (mapper == null) {
           t = (T) entries;
         } else {
           t = mapper.convertValue(entries, entityType);
         }
         if (filterFunction == null) {
           result.add(t);
         } else {
           Boolean apply = filterFunction.apply(t, key);
           if (Boolean.TRUE.equals(apply)) {
             result.add(t);
           }
         }
       }
     } finally {
       RedisConnectionUtils.releaseConnection(connection, factory);
     }
     return result;
   }


   /**
    * KeyでStringでRedisに保管する.
    *
    * @param key1  一つ目Key
    * @param value 値
    */
   public void putString(String key1, String value) {
     putString0(value, key1);
   }

   public void putString(String key1, String value, long timeOut) {
     stringRedisTemplate.opsForValue().set(key1, value, timeOut, TimeUnit.SECONDS);
   }

   /**
    * 2個KeyでStringでRedisに保管する.
    *
    * @param key1  一つ目Key
    * @param key2  二つ目Key
    * @param value 値
    */
   public void putString(String key1, String key2, String value) {
     putString0(value, key1, key2);
   }

   /**
    * 3個KeyでStringでRedisに保管する.
    *
    * @param key1  一つ目Key
    * @param key2  二つ目Key
    * @param key3  三つ目Key
    * @param value 値
    */
   public void putString(String key1, String key2, String key3, String value) {
     putString0(value, key1, key2, key3);
   }

   /**
    * Stringをプットする.
    *
    * @param keys  キー
    * @param value 値
    */
   public void putString(String[] keys, String value) {
     putString0(value, keys);
   }

   /**
    * Stringをフェッチする.
    *
    * @param key1 キー
    * @return 値
    */
   public String getString(String key1) {
     return getString0(key1);
   }

   /**
    * Stringをフェッチする.
    *
    * @param key1 key1 キー1
    * @param key2 key2 キー2
    * @return 値
    */
   public String getString(String key1, String key2) {
     return getString0(key1, key2);
   }

   /**
    * Stringをフェッチする.
    *
    * @param key1 key1 キー1
    * @param key2 key2 キー2
    * @param key3 key3  キー3
    * @return 値
    */
   public String getString(String key1, String key2, String key3) {
     return getString0(key1, key2, key3);
   }

   /**
    * Stringをフェッチする.
    *
    * @param keys キャッシュキー
    * @return 値
    */
   public String getString(String[] keys) {
     return getString0(keys);
   }

   /**
    * 八シューをプットする.
    *
    * @param cacheKey1 キャッシュキー
    * @param hashKey   八シューキー
    * @param hashValue 八シュー値
    */
   public void putHash(String cacheKey1, String hashKey, String hashValue) {
     putHash0(hashKey, hashValue, cacheKey1);
   }

   /**
    * 八シューをプットする.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param hashKey   八シューキー
    * @param hashValue 八シュー値
    */
   public void putHash(String cacheKey1, String cacheKey2, String hashKey, String hashValue) {
     putHash0(hashKey, hashValue, cacheKey1, cacheKey2);
   }

   /**
    * 八シューをプットする.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param cacheKey3 キャッシュキー3
    * @param hashKey   八シューキー
    * @param hashValue 八シュー値
    */
   public void putHash(String cacheKey1, String cacheKey2, String cacheKey3, String hashKey,
                       String hashValue) {
     putHash0(hashKey, hashValue, cacheKey1, cacheKey2, cacheKey3);
   }

   /**
    * 八シューをプットする.
    *
    * @param cacheKeys キャッシュキー
    * @param hashKey   八シューキー
    * @param hashValue 八シュー値
    */
   public void putHash(String[] cacheKeys, String hashKey, String hashValue) {
     putHash0(hashKey, hashValue, cacheKeys);
   }

   /**
    * 八シューを削除する.
    *
    * @param cacheKey1 キャッシュキー1
    * @param hashKey   八シューキー
    * @return 削除された件数
    */
   public long deleteHash(String cacheKey1, String hashKey) {
     return deleteHash0(hashKey, cacheKey1);
   }

   /**
    * 八シューを削除する.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param hashKey   八シューキー
    * @return 削除された件数
    */
   public long deleteHash(String cacheKey1, String cacheKey2, String hashKey) {
     return deleteHash0(hashKey, cacheKey1, cacheKey2);
   }

   /**
    * 八シューを削除する.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param cacheKey3 キャッシュキー3
    * @param hashKey   八シューキー
    * @return 削除された件数
    */
   public long deleteHash(String cacheKey1, String cacheKey2, String cacheKey3, String hashKey) {
     return deleteHash0(hashKey, cacheKey1, cacheKey2, cacheKey3);
   }

   /**
    * 八シューを削除する.
    *
    * @param cacheKeys キャッシュキー
    * @param hashKey   八シューキー
    * @return 削除された件数
    */
   public long deleteHash(String[] cacheKeys, String hashKey) {
     return deleteHash0(hashKey, cacheKeys);
   }

   /**
    * 八シューをフェッチする.
    *
    * @param cacheKey1 キャッシュキー
    * @param hashKey   八シューキー
    * @return 八シュー値
    */
   public String getHash(String cacheKey1, String hashKey) {
     return getHash0(hashKey, cacheKey1);
   }

   /**
    * 八シューをフェッチする.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param hashKey   八シューキー
    * @return 八シュー値
    */
   public String getHash(String cacheKey1, String cacheKey2, String hashKey) {
     return getHash0(hashKey, cacheKey1, cacheKey2);
   }

   /**
    * 八シューをフェッチする.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param cacheKey3 キャッシュキー3
    * @param hashKey   八シューキー
    * @return 八シュー値
    */
   public String getHash(String cacheKey1, String cacheKey2, String cacheKey3, String hashKey) {
     return getHash0(hashKey, cacheKey1, cacheKey2, cacheKey3);
   }

   /**
    * 八シューをフェッチする.
    *
    * @param cacheKeys キャッシュキー
    * @param hashKey   八シューキー
    * @return 八シュー値
    */
   public String getHash(String[] cacheKeys, String hashKey) {
     return getHash0(hashKey, cacheKeys);
   }

   /**
    * 切れた時間をセットする.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param cacheKey3 キャッシュキー3
    * @param timeout   タイムアウト
    * @param unit      単位
    * @return 成功結果
    */
   public boolean setExpire(String cacheKey1, String cacheKey2, String cacheKey3, long timeout,
                            TimeUnit unit) {
     return setExpire0(timeout, unit, cacheKey1, cacheKey2, cacheKey3);
   }

   /**
    * 切れた時間をセットする.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param timeout   タイムアウト
    * @param unit      単位
    * @return 成功結果
    */
   public boolean setExpire(String cacheKey1, String cacheKey2, long timeout, TimeUnit unit) {
     return setExpire0(timeout, unit, cacheKey1, cacheKey2);
   }

   /**
    * 切れた時間をセットする.
    *
    * @param cacheKey1 キャッシュキー1
    * @param timeout   タイムアウト
    * @param unit      単位
    * @return 成功結果
    */
   public boolean setExpire(String cacheKey1, long timeout, TimeUnit unit) {
     return setExpire0(timeout, unit, cacheKey1);
   }


   /**
    * キャッシュを削除する.
    *
    * @param cacheKey1 キャッシュキー1
    * @return 成功結果
    */
   public boolean deleteKey(String cacheKey1) {
     return deleteKey0(cacheKey1);
   }

   /**
    * キャッシュを削除する.
    *
    * @param cacheKey1 キャッシュキー1
    * @return 成功結果
    */
   public boolean deleteKey(String cacheKey1, String cacheKey2) {
     return deleteKey0(cacheKey1, cacheKey2);
   }

   /**
    * キャッシュを削除する.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param cacheKey3 キャッシュキー3
    * @return 成功結果
    */
   public boolean deleteKey(String cacheKey1, String cacheKey2, String cacheKey3) {
     return deleteKey0(cacheKey1, cacheKey2, cacheKey3);
   }

   /**
    * キャッシュをパータンで削除する.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param cacheKey3 キャッシュキー3
    * @return 件数
    */
   public long deletePattern(String cacheKey1, String cacheKey2, String cacheKey3) {
     Set<String> keys = redisTemplate.keys(connectKeys(cacheKey1, cacheKey2, cacheKey3));
     Long cnt = null;
     if (keys != null && keys.size() > 0) {
       cnt = redisTemplate.delete(keys);
     }
     return cnt == null ? 0 : cnt;
   }

   /**
    * キャッシュキーの存在を確認する.
    *
    * @param cacheKey1 キャッシュキー1
    * @param cacheKey2 キャッシュキー2
    * @param cacheKey3 キャッシュキー3
    * @return 存在結果
    */
   public boolean hasKey(String cacheKey1, String cacheKey2, String cacheKey3) {
     return Boolean.TRUE.equals(redisTemplate.hasKey(connectKeys(cacheKey1, cacheKey2, cacheKey3)));
   }

   public boolean hasKey(String cacheKey1) {
     return Boolean.TRUE.equals(redisTemplate.hasKey(connectKeys(cacheKey1)));
   }

   /**
    * リスト形に追加する.
    *
    * @param key1   キャッシュキー1
    * @param key2   キャッシュキー2
    * @param key3   キャッシュキー3
    * @param values キャッシュ値
    * @return 追加件数
    */
   public long addList(String key1, String key2, String key3, Collection<String> values) {
     Long aLong = redisTemplate.opsForList().rightPushAll(connectKeys(key1, key2, key3), values);
     return aLong == null ? 0L : aLong;
   }

   /**
    * キャシューからリスト値を取得する.
    *
    * @param key1 キャッシュキー1
    * @param key2 キャッシュキー2
    * @param key3 キャッシュキー3
    * @return 取得したリスト
    */
   public List<String> getList(String key1, String key2, String key3) {
     return redisTemplate.opsForList().range(connectKeys(key1, key2, key3), 0, -1);
   }

   /**
    * Stringの値を取得する.
    *
    * @param keys RedisのKey
    * @return 値
    */
   private String getString0(String... keys) {
     return stringRedisTemplate.opsForValue().get(connectKeys(keys));
   }

   /**
    * StringでRedisに保管する.
    *
    * @param value 値
    * @param keys  RedisのKey
    */
   private void putString0(String value, String... keys) {
     stringRedisTemplate.opsForValue().set(connectKeys(keys), value);
   }

   /**
    * Hashを削除する.
    *
    * @param hashKey HashのKey
    * @param keys    RedisのKey
    * @return 削除した件数
    */
   private long deleteHash0(String hashKey, String... keys) {
     return redisTemplate.opsForHash().delete(connectKeys(keys), hashKey);
   }

   /**
    * HashでRedisに保管する.
    *
    * @param hashKey   HashのKey
    * @param hashValue Hashの値
    * @param keys      RedisのKey
    */
   private void putHash0(String hashKey, String hashValue, String... keys) {
     redisTemplate.opsForHash().put(connectKeys(keys), hashKey, hashValue);
   }

   /**
    * Hash対象を取得する.
    *
    * @param hashKey HashのKey
    * @param keys    RedisのKey
    * @return Hashの値
    */
   private String getHash0(String hashKey, String... keys) {
     return (String) redisTemplate.opsForHash().get(connectKeys(keys), hashKey);
   }

   private boolean setExpire0(long timeout, TimeUnit unit, String... cacheKey) {
     return Boolean.TRUE.equals(redisTemplate.expire(connectKeys(cacheKey), timeout, unit));
   }

   private boolean deleteKey0(String... cacheKey) {
     return Boolean.TRUE.equals(redisTemplate.delete(connectKeys(cacheKey)));
   }

   /**
    * String配列よりRedisのKeyを「:」で繋がる.
    *
    * @param keys RedisのKeyの配列
    * @return RedisのKey
    */
   private String connectKeys(String... keys) {
     if (!ObjectUtils.isEmpty(keys) && !ObjectUtils.isEmpty(keys[0])) {
       return Arrays.stream(keys).reduce((pre, nxt) -> pre + ":" + nxt).orElse("");
     }
     return "";
   }
 }
