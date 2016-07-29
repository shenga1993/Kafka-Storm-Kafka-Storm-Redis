package AudienceRating;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/*
 * reportTime 时间戳|msisdn 手机号码|apmac|acmac 访问ip地址|host 访问域名|siteType 网址类型|upPackNum 上行数据包数量|downPackNum|upPayLoad 上行总流量|downPayLoad|httpStatus response状态
 */
public class MakeLogDate {
	/*
	 * 因为是模拟实时数据，将时间限定在特定日期的特定时间范围内(9:00-11:00)
	 */
	private static final DecimalFormat DATE_FORMAT = new DecimalFormat("#00");
	private static final String YEAR = "2015";
	private static final String MONTH = "11";
	private static final String DAY = "27";
	private static final int[] HOURS = { 9, 10, 11 };
	public static final Random RDOM = new Random();
	public static final String[] A = { "VOD", "LIVOD" };
	public static final String[] CHANNEL = { "CCTV-5", "CCTV-1" , "CCTV-2", "CCTV-3", "CCTV-4", "CCTV-6", "CCTV-7", "CCTV-8", "CCTV-9"};
	public static final String[] AN = { "军事节目", "美食节目", "综艺节目", "新闻在线", "幽默娱乐", "时事政治", "非常时刻", "相亲节目", "明星采访" };
	private static final String[] AP_RIGHT = { "7Daysinn", "CMCC-EASY", "CMCC", "CMCC-EDU" };

	public static void main(String[] args) {

		send_Product("main producer is over!");
	}

	private String make_reportTime() {
		String hour = DATE_FORMAT.format(HOURS[RDOM.nextInt(3)]);
		String min = DATE_FORMAT.format(RDOM.nextInt(61));
		String sec = DATE_FORMAT.format(RDOM.nextInt(61));
		String time = YEAR + MONTH + DAY + hour + min + sec;
		return time;
	}

	private static void send_Product(String who) {
		Properties pro = new Properties();
		pro.put("zookeeper.connect", "localhost:2181");
		pro.put("serializer.class", "kafka.serializer.StringEncoder");
		pro.put("producer.type", "async");
		pro.put("compression.codec", "1");
		pro.put("metadata.broker.list", "localhost:9092");
		ProducerConfig config = new ProducerConfig(pro);
		Producer<String, String> producer = new Producer<String, String>(config);
		int line_num = 50000;
		MakeLogDate md = new MakeLogDate();
		for (int i = 0; i <= line_num; i++) {
			StringBuilder sb = new StringBuilder();
			String type = A[RDOM.nextInt(A.length)];
			String A = RDOM.nextInt(5) + "";
			String mac = md.make_apmac();
			String pl = RDOM.nextInt(4) + "";
			String pt = RDOM.nextInt(4) + "";
			String channel = CHANNEL[RDOM.nextInt(CHANNEL.length)];
			String ts = md.make_reportTime();
			String an = AN[RDOM.nextInt(AN.length)];
			sb.append(type).append("|").append(A).append("|").append(mac).append("|").append(pl).append("|").append(pt)
					.append("|").append(channel).append("|").append(ts).append("|").append(an);
//			System.out.println(sb.toString());
			producer.send(new KeyedMessage<String, String>("logtest", sb.toString()));
			
			
			// try {
			// Thread.sleep(10);
			// } catch (InterruptedException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		}
		producer.close();
		System.out.println(who);
	}

	private String make_apmac() {
		/* 用来产生:左边的随机数列 */
		StringBuilder left = new StringBuilder();
		for (int i = 0; i < 6; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = 0; j < 2; j++) {
				int choose = RDOM.nextInt(2);
				switch (choose) {
				case 0:
					int fromchar = 'a';
					int endchar = 'z';
					int need = 0;
					while (need < fromchar) {
						need = RDOM.nextInt(endchar);
					}
					char f = (char) need;
					sb.append(f + "");
					break;

				case 1:
					int need1 = RDOM.nextInt(10);
					sb.append(need1 + "");
					break;
				}
			}
			left.append(sb.toString() + "-");
		}
		/* 用来产生:右边的随机数列 */
		String right = AP_RIGHT[RDOM.nextInt(3)];
		return left.deleteCharAt(left.length() - 1).toString() + ":" + right;

	}
}
