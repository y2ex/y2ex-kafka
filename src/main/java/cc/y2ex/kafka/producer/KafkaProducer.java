package cc.y2ex.kafka.producer;

import cc.y2ex.kafka.constants.KafkaConstant;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Yanci丶
 * @date 2020-12-09
 */
@Slf4j
@Component
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void send(Object obj) {
        String obj2String = JSONObject.toJSONString(obj);
        log.info("start send message:{}", obj2String);
        //send message
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaConstant.TOPIC_TEST, obj);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.info("{} - producer send message failed, e:{}", KafkaConstant.TOPIC_TEST, throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> stringObjectSendResult) {
                log.info("{} - producer send message success, sendResult:{}", KafkaConstant.TOPIC_TEST, stringObjectSendResult.toString());
            }
        });
    }
}
