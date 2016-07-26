package redisDemo;

import redis.clients.jedis.Jedis;
import redisUtils.RedisUtils;

public class RedisDemo {

	public static void main(String[] args) {
		// TODO 自动生成的方法存根
		Jedis jedis = RedisUtils.redisConnect("localhost");
		RedisUtils.setKV(jedis,"gitv","叶昇");
		System.out.println(RedisUtils.getValue(jedis, "gitv"));
	}
}
