package com.linkedin.camus.etl.kafka.coders;

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;

public class CustomDecoder extends MessageDecoder<Message, String>
{
	private static final org.apache.log4j.Logger log = Logger.getLogger(CustomDecoder.class);
	@Override
	public CamusWrapper<String> decode(Message message)
	{
		// TODO Auto-generated method stub
		long timestamp = 0;
		String key="Error_1_BADRECS";
		String payloadString=null;
		try
		{
			payloadString = new String(message.getPayload(), "UTF-8");
			key = payloadString.split(",")[0];
			timestamp = System.currentTimeMillis();
		}
		catch (UnsupportedEncodingException e)
		{
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
		catch(ArrayIndexOutOfBoundsException e)
		{
			log.error("Arry out of bounds.Record is"+payloadString);
		}
		catch(Exception e)
		{
			log.error(e.getMessage()+"Error generating rec"+payloadString);
		}
		// Key will be RegionName_#_day_#,where # is a number for well and day
		// respectively.E.g Welsh_1_Day_1
		log.info(payloadString+","+timestamp+" key:"+key);
		return (new CamusWrapper<String>(payloadString, timestamp, key, ""));
	}

}
