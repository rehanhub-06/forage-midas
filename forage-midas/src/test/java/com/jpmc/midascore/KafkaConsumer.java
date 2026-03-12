package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class KafkaConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${midas.kafka.topic}")
    public void listen(String message) {
        try {
            Transaction transaction =
                    objectMapper.readValue(message, Transaction.class);

            // DO NOTHING for now

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}