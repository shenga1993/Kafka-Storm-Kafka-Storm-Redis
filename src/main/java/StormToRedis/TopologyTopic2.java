package StormToRedis;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import redis.clients.jedis.Jedis;
import redisUtils.RedisUtils;

public class TopologyTopic2 {
	
	public static class MyBolt extends BaseBasicBolt {

		/**
		 * 
		 */
		
		private Jedis jedis = null;
		private static int count = 0;
		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO 自动生成的方法存根
			super.prepare(stormConf, context);
			try{
				jedis = RedisUtils.redisConnect("localhost");
			}
			catch(Exception e){
				throw new RuntimeException();
			}
		}
		
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO 自动生成的方法存根
			String msg = (String) input.getValue(0);
			count=count+1;
			jedis.set(count+"",msg);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO 自动生成的方法存根
			
		}
		
		@Override
		public void cleanup() {
			// TODO 自动生成的方法存根
			jedis.close();
		}

	}

	public static class MyScheme implements Scheme {

		/**
		 * 
		 */

		@Override
		public List<Object> deserialize(ByteBuffer ser) {
			// TODO 自动生成的方法存根
			byte[] bufr = ser.array();
			try {
				String msg = new String(bufr,"UTF-8");
				return new Values(msg);
			} catch (UnsupportedEncodingException e) {
				// TODO 自动生成的 catch 块
				
			}
			return null;
		}

		@Override
		public Fields getOutputFields() {
			// TODO 自动生成的方法存根
			return new Fields("msg");
		}
		
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		// TODO 自动生成的方法存根
		TopologyBuilder builder = new TopologyBuilder();
		BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "topic2","/storm","spout2");
		Config conf = new Config();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.broker.list","localhost:9092");
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
		spoutConfig.scheme = new SchemeAsMultiScheme(new MyScheme());
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt", new MyBolt()).shuffleGrouping("spout");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("topo2", conf, builder.createTopology());
		Utils.sleep(100000);
		cluster.killTopology("topo2");
		cluster.shutdown();
		
	}

}
