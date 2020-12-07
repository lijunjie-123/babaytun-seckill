package com.itlaoqi.babytunseckill.service;

import com.alibaba.fastjson.JSONObject;
import com.itlaoqi.babytunseckill.dao.OrderDAO;
import com.itlaoqi.babytunseckill.entity.Order;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Date;

@Component
public class OrderConsumer {


    private DefaultMQPushConsumer consumer;


    private String consumerGroup = "pay_consumer_seckill_order";

    @Resource
    private OrderDAO orderDAO;


    //并发消费
    public OrderConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr("192.168.188.129:9876;192.168.188.130:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("pay_seckill_order", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, contxt) -> {
            MessageExt message = msgs.get(0);
            int time = message.getReconsumeTimes();
            try {
                String body = new String(message.getBody(), "utf-8");

                //模拟消息重试
               /* if(keys.equals("xianyue667788")){
                    throw new RuntimeException();
                }*/

                JSONObject json = JSONObject.parseObject(body);

                Order order = new Order();
                order.setOrderNo((String) json.get("orderNo"));
                order.setOrderStatus(0);
                order.setUserid((String) json.get("userid"));
                order.setRecvName("xxx");
                order.setRecvMobile("1393310xxxx");
                order.setRecvAddress("xxxxxxxxxx");
                order.setAmout(19.8f);
                order.setPostage(0f);
                order.setCreateTime(new Date());
                orderDAO.insert(order);
                System.out.println((String) json.get("orderNo") + "订单已创建");

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
                if (time >= 2) {
                    //数据存到数据库或者通知运营处理，返回成功
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

                System.out.println("消息重试中，重试次数为：" + time);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
    }




    /*@RabbitListener(
            bindings = @QueueBinding(
                    value = @Queue(value = "queue-order") ,
                    exchange = @Exchange(value = "exchange-order" , type = "fanout")
            )
    )
    @RabbitHandler
    public void handleMessage(@Payload Map data , Channel channel ,
                              @Headers Map<String,Object> headers){
        System.out.println("=======获取到订单数据:" + data + "===========);");

        try {
            //对接支付宝、对接物流系统、日志登记。。。。
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Order order = new Order();
            order.setOrderNo(data.get("orderNo").toString());
            order.setOrderStatus(0);
            order.setUserid(data.get("userid").toString());
            order.setRecvName("xxx");
            order.setRecvMobile("1393310xxxx");
            order.setRecvAddress("xxxxxxxxxx");
            order.setAmout(19.8f);
            order.setPostage(0f);
            order.setCreateTime(new Date());
            orderDAO.insert(order);
            Long tag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
            channel.basicAck(tag , false);//消息确认
            System.out.println(data.get("orderNo") + "订单已创建");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

}
