package AudienceRating;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;

import StormToRedis.TestTopology.MyBolt;
import StormToRedis.TestTopology.MyScheme;
import redis.clients.jedis.Jedis;
import zkUtils.ZkUtils;

public class AudienceRatingTopology {

	private final static String TOTAL = "total";

	public static class RealTimeBolt extends BaseRichBolt {

		private OutputCollector collector;
		private Jedis jedis;
		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String log = input.getString(0);
			String[] columns = log.split("\\|");
			if (columns.length == 8 && columns[0].toUpperCase().equals("LIVOD") && columns[1].equals("5")) {
				String vodtype = columns[0];
				String action = columns[1];
				String mac = columns[2];
				String albumId = columns[7];
				String chnName = columns[5];
				String pl = columns[3];
				String pt = columns[4];
				String userTs = columns[6];
				if (pt.equals("0")) {
					jedis.incr(TOTAL);
					jedis.incr(chnName);
					collector.emit(new Values(albumId, chnName, userTs));
					collector.ack(input);

				} else if (pt.equals("1") || pt.equals("2")) {
					jedis.decr(TOTAL);
					jedis.decr(chnName);
					collector.emit(new Values(albumId, chnName, userTs));
					collector.ack(input);
				}
			}
		}

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
			jedis = new Jedis("localhost");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("albumId", "chnName", "userTs"));
		}

	}

	public static class AudienceRatingBolt extends BaseRichBolt {

		private OutputCollector collector;
		private Jedis jedis;
		@SuppressWarnings("unchecked")
		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String albumId = input.getStringByField("albumId");
			String chnName = input.getStringByField("chnName");
			String userTs = input.getStringByField("userTs");
			Integer total = Integer.parseInt(jedis.get(TOTAL));
			Integer chncount = Integer.parseInt(jedis.get(chnName));
			double audienceRatingPercent = (double) chncount / (double) total;
			BigDecimal bigDecimal = new BigDecimal(audienceRatingPercent);
			audienceRatingPercent = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("userTs",userTs);
			jsonObject.put("chnName",chnName);
			jsonObject.put("albumId",albumId);
			jsonObject.put("audienceRatingPercent",audienceRatingPercent);
			jedis.hset(userTs, chnName,jsonObject.toJSONString());
			collector.emit(new Values(userTs, chnName, albumId));
			collector.ack(input);
		}

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
			jedis = new Jedis("localhost");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("userTs", "chnName", "albumId"));
		}

	}

//	public static class CassandraInsertBolt extends BaseRichBolt {
//
//		private OutputCollector collector;
//
//		@Override
//		public void execute(Tuple input) {
//			// TODO Auto-generated method stub
//			String userTs = input.getStringByField("userTs");
//			String albumId = input.getStringByField("albumId");
//			String chnName = input.getStringByField("chnName");
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
//			try {
//				Date date = sdf.parse(userTs);
//				
//			} catch (ParseException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//			
//			
//		}
//
//		@Override
//		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//			// TODO Auto-generated method stub
//			this.collector = collector;
//		}
//
//		@Override
//		public void declareOutputFields(OutputFieldsDeclarer declarer) {
//			// TODO Auto-generated method stub
//			
//		}
//
//	}
	
	public static class MyScheme implements Scheme {

		/**
		 * 
		 */

		@Override
		public List<Object> deserialize(ByteBuffer ser) {
			// TODO 自动生成的方法存根
			if(ser!=null){
				byte[] bytes = ser.array();
				try 
				{
					String msg = new String(bytes, "UTF-8");
					return new Values(msg);
				} catch (UnsupportedEncodingException e) {
					// TODO 自动生成的 catch 块
					
				}
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
		// TODO Auto-generated method stub
		BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "logtest", "/brokers", "spout");
		Config conf = new Config();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.broker.list","localhost:9092");
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
//		Properties props = new Properties();
//		props.put("bootstrap.servers", "localhost:9092");
//        props.put("acks", "1");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt1", new RealTimeBolt()).shuffleGrouping("spout");
		builder.setBolt("bolt2", new AudienceRatingBolt()).shuffleGrouping("bolt1");
		StormSubmitter.submitTopology("logto", conf, builder.createTopology());
	}

}
