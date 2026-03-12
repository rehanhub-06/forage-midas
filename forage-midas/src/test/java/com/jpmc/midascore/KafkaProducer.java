package com.jpmc.midascore;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class KafkaProducer {

    private final String topic;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaProducer(
            @Value("${midas.kafka.topic}") String topic,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        try {
            String[] data = transactionLine.split(", ");

            Transaction transaction = new Transaction(
                    Long.parseLong(data[0]),
                    Long.parseLong(data[1]),
                    Float.parseFloat(data[2])
            );

            String json = objectMapper.writeValueAsString(transaction);
            kafkaTemplate.send(topic, json); 

        } catch (Exception e) {
            throw new RuntimeException("Failed to send transaction", e);
        }
    }
}