package com.poc.beam.beampoc;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class IotDeserializer implements Deserializer<IotEvent> {
	
 @Override
public void close() {
	// TODO Auto-generated method stub
	Deserializer.super.close();
}
 
 @Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		Deserializer.super.configure(configs, isKey);
	}
 
 
 @Override
	public IotEvent deserialize(String args, byte[] args1) {
		// TODO Auto-generated method stub
	 	
	 ObjectMapper om = new ObjectMapper();
	 IotEvent iotEvent=null;
	 try {
		 iotEvent = om.readValue(args1, IotEvent.class);
	 }
	 catch(Exception e) {
		 System.out.println(e.getMessage());
	 }
	 return iotEvent;
	}
 
}

