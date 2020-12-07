package com.itlaoqi.babytunseckill.message;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;

/**
 * Created by wangxy on 20/11/24.
 */
@Component
public class PayProducer {

    private String producerGroup = "pay_seckill_group";


    private String nameServerAddr = "192.168.188.129:9876;192.168.188.130:9876";


    private DefaultMQProducer producer;


    public PayProducer(){
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(nameServerAddr);
        start();
    }


    public void start(){
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public void shutdown (){
        this.producer.shutdown();
    }


    public DefaultMQProducer getProducer(){

        return this.producer;
    }

}
