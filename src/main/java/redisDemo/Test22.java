package redisDemo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test22 {

	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
		Date date = sdf.parse("2016-10-18 10:55:22");
		System.out.println(date.getTime());
		date = sdf.parse("2016-10-18 10:56:12");
		long a = date.getTime()+1000;
		date.setTime(a);
		System.out.println(date.toLocaleString());
		a=a+1000;
		date.setTime(a);
		System.out.println(date.getYear()+"-"+date.getMonth()+"-"+date.getDay()+"\t"+date.getHours()+":"+date.getMinutes()+":"+date.getSeconds());
		
	}	

}
