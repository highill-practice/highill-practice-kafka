package com.highill.practice.kafka;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerBase {

	public static <K, V> KafkaProducer<K, V> producer(String serverConfig, String clientId, String keySerializer, String valueSerializer) {
		KafkaProducer<K, V> kafkaProducer = null;
		if (serverConfig != null && clientId != null) {
			Properties config = new Properties();
			config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverConfig);
			config.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
			// config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
			// config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
			config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
			config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

			kafkaProducer = new KafkaProducer<K, V>(config);
		}
		return kafkaProducer;
	}
	
	public static <K, V> Future<RecordMetadata> send(String topicName, KafkaProducer<K, V> kafkaProducer, K key, V value) {
		Future<RecordMetadata> futureMetadata = null;
		if(topicName != null && kafkaProducer != null && key != null && value != null) {
			ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topicName, key, value);
			futureMetadata =  kafkaProducer.send(producerRecord);
			if(futureMetadata != null) {
				System.out.println("-----futureMetadata: " + futureMetadata);
			}
		}
		return futureMetadata;
	}
	
	public static <K, V> Future<RecordMetadata> send(String topicName, KafkaProducer<K, V> kafkaProducer, K key, V value, Callback callback) {
		Future<RecordMetadata> futureMetadata = null;
		if(topicName != null && kafkaProducer != null && key != null && value != null && callback != null ) {
			ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topicName, key, value);
			futureMetadata = kafkaProducer.send(producerRecord, callback);
		}
		return futureMetadata;
	}

}
