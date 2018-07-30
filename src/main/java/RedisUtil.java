import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RedisUtil {

    private static final Logger log = LoggerFactory.getLogger(GrouponRedisCacheUtil.class);

    @Autowired
    private RedisCacheUtil redisCacheUtil;
    @Autowired
    private ICacheManager iCacheManager;
    /**
     *
     * @Description: 为null对象时，设置一分钟缓存
     * @param <T>
     * @param <K>
     * @param t 设置对象
     * @param redisCacheKeyEnum 枚举类型
     * @param k
     */
    @SuppressWarnings("unchecked")
    public <T, K> void setValueOneMinute(T t, RedisCacheKeyEnum redisCacheKeyEnum, K... k) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum, k);
            RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key,t,60);
            this.redisCacheUtil.setValue(redisParam);
        } catch (Exception e) {
            log.error("setValue is error key is " + key, e);
        }
    }

    /**
     *
     * @Description: 在缓存中设置对象
     * @param <T>
     * @param <V>
     * @param t 设置对象
     * @param redisCacheKeyEnum 枚举类型
     * @param v 变长参数
     */
    @SuppressWarnings("unchecked")
    public <T, V> void setValue(T t, RedisCacheKeyEnum redisCacheKeyEnum, V... v) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum, v);
            RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key,t,redisCacheKeyEnum.getSecond());
            this.redisCacheUtil.setValue(redisParam);
        } catch (Exception e) {
            log.error("setValue is error key is " + key, e);
        }
    }
    //该方法的存在主要用于需要在redis里面进行incr操作的缓存
    //注意放的时候和取的时候,template的一致性
    public <Integer, V> void setIntegerValue(Integer t, RedisCacheKeyEnum redisCacheKeyEnum, V... v) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum, v);
            String cacheKey=redisCacheKeyEnum.getPrefix()+":"+key;
            this.iCacheManager.getJedisCluster().setex(cacheKey, redisCacheKeyEnum.getSecond(), String.valueOf(t));
        } catch (Exception e) {
            log.error("setValue is error key is " + key, e);
        }
    }
    /**
     *
     * @Description: 在缓存中获取对象
     * @param <T>
     * @param <V>
     * @param redisCacheKeyEnum 枚举类型
     * @param v 变长参数
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T, V> T getValue(RedisCacheKeyEnum redisCacheKeyEnum, V... v) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum, v);
            RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key);
            return (T)this.redisCacheUtil.getValue(redisParam);
        } catch (Exception e) {
            log.error("getValue is error key is " + key, e);
            return null;
        }
    }
    public <V> Integer getIntegerValue(RedisCacheKeyEnum redisCacheKeyEnum, V... v) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum, v);
            String cacheKey=redisCacheKeyEnum.getPrefix()+":"+key;
            String result=this.iCacheManager.getJedisCluster().get(cacheKey);
            if(result!=null){
                return Integer.valueOf(result);
            }
            return  null;
        } catch (Exception e) {
            log.error("getValue is error key is " + key, e);
            return null;
        }
    }
    public <V> Long getLongValue(RedisCacheKeyEnum redisCacheKeyEnum, V... v) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum, v);
            String cacheKey=redisCacheKeyEnum.getPrefix()+":"+key;
            String result=this.iCacheManager.getJedisCluster().get(cacheKey);
            if(result!=null){
                return Long.valueOf(result);
            }
            return  null;
        } catch (Exception e) {
            log.error("getValue is error key is " + key, e);
            return null;
        }
    }
    /**
     *
     * @Description: 移除对象
     * @param redisCacheKeyEnum
     */
    public <K> void remove(RedisCacheKeyEnum redisCacheKeyEnum,K... k) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum,k);
            RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key);
            this.redisCacheUtil.remove(redisParam);
        } catch (Exception e) {
            log.error("remove is error key is "+key, e);
        }
        this.removeInteger(redisCacheKeyEnum,k);
    }
    private  <K> void removeInteger(RedisCacheKeyEnum redisCacheKeyEnum,K... k) {
        String key = null;
        try {
            key = generateKey(redisCacheKeyEnum, k);
            String cacheKey=redisCacheKeyEnum.getPrefix()+":"+key;
            this.iCacheManager.getJedisCluster().del(cacheKey);
        } catch (Exception e) {
            log.error("removeIntegerCache is error key is " + key, e);
        }
    }
    /**
     *
     * @Description: 通过枚举跟参数生成缓存所需要的key
     * @param <K>
     * @param redisCacheKeyEnum 枚举类型
     * @param k 变长参数
     * @return
     */
    @SuppressWarnings("unchecked")
    public <K> String generateKey(RedisCacheKeyEnum redisCacheKeyEnum, K... k) {
        if (k == null || k.length == 0) {
            return redisCacheKeyEnum.getKey();
        }
        Object[] s = new Object[k.length];
        for (int i = 0; i < k.length; i++) {
            if (null != k[i]) {
                s[i] = String.valueOf(k[i]);
            }
        }
        return MessageFormat.format(redisCacheKeyEnum.getKey(), s);
    }

    public boolean lock(String key)
    {
        return this.redisCacheUtil.lock(key);
    }

    public boolean unLock(String key)
    {
        if (StringUtils.isBlank(key)){
            return false;
        }
        try
        {
            boolean result = this.redisCacheUtil.unlock(key);
            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public <K> Long incr(RedisCacheKeyEnum redisCacheKeyEnum, K... k)
    {
        try{
            String key = generateKey(redisCacheKeyEnum,k);
            String cacheKey=redisCacheKeyEnum.getPrefix()+":"+key;
            Long result=this.iCacheManager.getJedisCluster().incr(cacheKey);
            return result;
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
        return -1L;
    }

    /**
     * 列表操作--追加
     */
    public <T, K> boolean rightPush(T t,RedisCacheKeyEnum redisCacheKeyEnum, K... k){
        String key = generateKey(redisCacheKeyEnum,k);
        RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key,t,redisCacheKeyEnum.getSecond());
        return this.redisCacheUtil.rightPush(redisParam);
    }
    public <K> Long getListSize(RedisCacheKeyEnum redisCacheKeyEnum, K... k){
        String key = generateKey(redisCacheKeyEnum,k);
        RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key);
        return this.redisCacheUtil.getListSize(redisParam);
    }
    public <T, K> boolean removeValueFromList(T t,RedisCacheKeyEnum redisCacheKeyEnum, K... k){
        String key = generateKey(redisCacheKeyEnum,k);
        RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key,t,redisCacheKeyEnum.getSecond());
        return this.redisCacheUtil.removeFromList(redisParam);
    }
    public <T, K> boolean leftPush(T t,RedisCacheKeyEnum redisCacheKeyEnum, K... k){
        try{
            String key = generateKey(redisCacheKeyEnum,k);
            RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key,t,redisCacheKeyEnum.getSecond());
            Long num = this.iCacheManager.getJedisCluster().rpush(SerializingUtil.serialize(redisParam.getKey()), new byte[][]{SerializingUtil.serialize(redisParam.getValue())});
            return num.longValue() > 0L;
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
        return false;
    }
    public <K> List getLeftList(int size, RedisCacheKeyEnum redisCacheKeyEnum, K... k){
        try{
            String key = generateKey(redisCacheKeyEnum,k);
            RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key);
            List<byte[]> list = this.iCacheManager.getJedisCluster().lrange(SerializingUtil.serialize(redisParam.getKey()), 0, (long)size);
            if (null != list && list.size() > 0) {
                List<Object> objList = new ArrayList();
                Iterator var6 = list.iterator();
                while(var6.hasNext()) {
                    byte[] b = (byte[])var6.next();
                    objList.add(SerializingUtil.deserialize(b));
                }
                return objList;
            } else {
                return null;
            }
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
        return null;
    }
    public <K> boolean lTrimList(int size, RedisCacheKeyEnum redisCacheKeyEnum, K... k){
        try{
            String key = generateKey(redisCacheKeyEnum,k);
            RedisParam redisParam=RedisParam.of(redisCacheKeyEnum.getPrefix(),key);
            String result= this.iCacheManager.getJedisCluster().ltrim(SerializingUtil.serialize(redisParam.getKey()), 0, (long)size);
            return true;
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
        return false;
    }
}
