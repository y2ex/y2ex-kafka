package cc.y2ex.kafka.controller;

import cc.y2ex.kafka.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Yanci丶
 * @date 2020-12-09
 */
@Slf4j
@RestController
public class KafkaController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @GetMapping("/send")
    public String send(@RequestParam("msg") String msg){
        log.info("kafka send param, msg:{}", msg);
        kafkaProducer.send(msg);
        return "SUCCESS";
    }
}
