package AudienceRating;

public class LogProduceStart {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LogProducer producer = new LogProducer("logtest",true);
		new Thread(producer).start();
	}

}
