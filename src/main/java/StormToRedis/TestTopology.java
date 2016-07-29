package StormToRedis;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
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
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import zkUtils.ZkUtils;

public class TestTopology {
	
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

	public static class MyBolt extends BaseBasicBolt {
		
		
		/**
		 * 
		 */

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO 自动生成的方法存根
			String msg = (String) input.getValue(0);
			String out = "I'm" + msg +" .;";
			collector.emit(new Values(out));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO 自动生成的方法存根
			declarer.declare(new Fields("nmsg"));
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		// TODO 自动生成的方法存根
		BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "topic3", "/brokers", "spout");
		Config conf = new Config();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.broker.list","localhost:9092,localhost:9093,localhost:9094");
		map.put("serializer.class", ZkUtils.SERIALIZERCLASS);
		conf.put("kafka.broker.properties", map);
		conf.put("topic", "topic2");
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		spoutConfig.scheme = new SchemeAsMultiScheme(new MyScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt1", new MyBolt()).shuffleGrouping("spout");
		builder.setBolt("bolt2", new KafkaBolt<String, Integer>().withProducerProperties(props).withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>())).shuffleGrouping("bolt1");
		if(args==null||args.length==0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("topo", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("topo");
			cluster.shutdown();
		}
		else{
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
	}
}
