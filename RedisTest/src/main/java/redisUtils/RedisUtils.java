package redisUtils;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public class RedisUtils {
	private static final Log LOG = LogFactory.getLog(RedisUtils.class);
	public static Jedis redisConnect(String ip){
		return new Jedis(ip);
	}
	
	public static JedisCluster redisConnect(String... hostInfos){
		Set<HostAndPort> jedisClusterNodes = new HashSet<>();
		for(String hostInfo:hostInfos){
			if(hostInfo.split(":").length!=2){
				continue;
			}
			try{
				jedisClusterNodes.add(new HostAndPort(hostInfo.split(":")[0],Integer.parseInt(hostInfo.split(":")[1])));
			}
			catch(Exception e){
				continue;
			}
		}
		if(jedisClusterNodes.isEmpty()){
			throw new RuntimeException("Usage redisConnect(ip1:port1,ip2:port2...)");
		}
		JedisCluster jc = new JedisCluster(jedisClusterNodes);
		return jc;
	}
	
	public static void setKV(Jedis jedis,String key,String value){
		jedis.set(key, value);
		LOG.info("set key:" + key +" value= "+value);
	}
	
	public static String getValue(Jedis jedis,String key){
		return jedis.get(key);
	}
	
	public static void setKV(JedisCluster jc,String key,String value){
		jc.set(key, value);
		LOG.info("set key:" + key +" value= "+value);
	}
	
	public static String getValue(JedisCluster jc,String key){
		return jc.get(key);
	}
	
	public static void delKey(JedisCluster jc,String key){
		jc.del(key);
		LOG.info("key：" + key +" is deleted");
	}
	
	public static void delKey(Jedis jedis,String key){
		jedis.del(key);
		LOG.info("key：" + key +" is deleted");
	}
}
