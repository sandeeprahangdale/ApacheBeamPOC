package com.poc.beam.beampoc;

import java.sql.PreparedStatement;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Duration;


public class RealTimeStreamingETL {

	public static void main(String[] args) {
		Pipeline p = Pipeline.create();
		
		p.apply(KafkaIO.<Long,IotEvent>read()
				.withBootstrapServers("localhost:9092")
				.withTopic("tinput2")
				.withKeyDeserializer(LongDeserializer.class)
				.withValueDeserializer(IotDeserializer.class)
				.withoutMetadata()
		)
		.apply(Values.<IotEvent>create())
		.apply(Window.<IotEvent>into(FixedWindows.of(Duration.standardSeconds(10))))
		.apply(ParDo.of(new DoFn<IotEvent, String>() {
			
			@ProcessElement
			public void processElement(ProcessContext c) {
				
				if(c.element().getTemperature()>80.0) {
					c.output(c.element().getDeviceId());
				}
				
			}			
		}))
		.apply(Count.perElement())
		.apply(JdbcIO.<KV<String,Long>>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
				.create("com.microsoft.sqlserver.jdbc.SQLServerDriver","jdbc:sqlserver://localhost;databaseName=beamdb")
				.withUsername("sa")
				.withPassword("Test123@1"))
				.withStatement("insert into event values (?,?) ")
				.withPreparedStatementSetter(new PreparedStatementSetter<KV<String,Long>>() {
					
					public void setParameters(KV<String,Long> element, PreparedStatement preparedStatement) throws Exception {
						// TODO Auto-generated method stub
						preparedStatement.setString(1, element.getKey());
						preparedStatement.setLong(2, element.getValue());
					}
				})
				);
		
		
		p.run();
		


		
	}
}

