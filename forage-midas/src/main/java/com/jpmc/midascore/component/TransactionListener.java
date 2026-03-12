package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    @Value("${incentive.url}")
    private String incentiveUrl;

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${midas.kafka.topic}", groupId = "transaction-listener-group")
    public void listenString(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            UserRecord sender = userRepository.findById(transaction.getSenderId());
            UserRecord recipient = userRepository.findById(transaction.getRecipientId());

            if (sender != null && recipient != null) {
                if (sender.getBalance() >= transaction.getAmount()) {

                    Incentive incentive = restTemplate.postForObject(incentiveUrl, transaction, Incentive.class);
                    float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0f;

                    sender.setBalance(sender.getBalance() - transaction.getAmount());
                    recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

                    userRepository.save(sender);
                    userRepository.save(recipient);

                    TransactionRecord tr = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
                    transactionRecordRepository.save(tr);
                    logger.info("Processed valid transaction from {} to {} for {} with incentive {}", sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount);
                } else {
                    logger.info("Transaction discarded: insufficient balance for sender {}", sender.getName());
                }
            } else {
                logger.info("Transaction discarded: Invalid sender or recipient ID");
            }

        } catch (Exception e) {
            logger.error("Error processing transaction message", e);
        }
    }
}
