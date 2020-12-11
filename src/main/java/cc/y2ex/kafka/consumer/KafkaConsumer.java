package cc.y2ex.kafka.consumer;

import cc.y2ex.kafka.constants.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author Yanci丶
 * @date 2020-12-09
 */
@Slf4j
@Component
public class KafkaConsumer {

    @KafkaListener(topics = KafkaConstant.TOPIC_TEST, groupId = KafkaConstant.TOPIC_GROUP1)
    public void topicTest1(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        Optional message = Optional.ofNullable(record.value());
        if (message.isPresent()) {
            Object msg = message.get();
            log.info("{} 消费了, Topic:{}, Message:{}", KafkaConstant.TOPIC_GROUP1, topic, msg);
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = KafkaConstant.TOPIC_TEST, groupId = KafkaConstant.TOPIC_GROUP2)
    public void topicTest2(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        Optional message = Optional.ofNullable(record.value());
        if (message.isPresent()) {
            Object msg = message.get();
            log.info("{} 消费了, Topic:{}, Message:{}", KafkaConstant.TOPIC_GROUP2, topic, msg);
            ack.acknowledge();
        }
    }
}
