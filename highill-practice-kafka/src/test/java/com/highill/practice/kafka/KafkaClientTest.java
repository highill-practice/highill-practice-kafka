package com.highill.practice.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class KafkaClientTest {

	@Test
	public void message() {
		String topicName = "topicJavaClient2017";
		String serverUrl = "192.168.1.106:9092";

		String stringSerializer = "org.apache.kafka.common.serialization.StringSerializer";
		String producerKeySerializer = stringSerializer;
		String producerValueSerializer = stringSerializer;
		KafkaProducer<String, String> kafkaProducer = KafkaProducerBase.producer(serverUrl, "JavaClient", producerKeySerializer,
		        producerValueSerializer);
		System.out.println("-----kafkaProducer: " + kafkaProducer);

		String groupId = "JavaConsumeDemo";
		String stringDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
		String consumerKeyDeserializer = stringDeserializer;
		String consumerValueDeserializer = stringDeserializer;
		KafkaConsumer<String, String> kafkaConsumer = KafkaConsumerBase.consumer(serverUrl, groupId, 
				true, 1000, 1000 * 30,
		        consumerKeyDeserializer, consumerValueDeserializer);
		System.out.println("----- kafkaConsumer: " + kafkaConsumer);

		int messageRecord = 10;
		for (int size = 1; size <= messageRecord; size++) {
			KafkaConsumerBase.subscribe(topicName, kafkaConsumer, 1000);
			KafkaProducerBase.send(topicName, kafkaProducer, "id10000" + size,
			        "This is " + size + " a test message. " + System.currentTimeMillis());
		}
		KafkaConsumerBase.subscribe(topicName, kafkaConsumer, 1000);

	}

}
