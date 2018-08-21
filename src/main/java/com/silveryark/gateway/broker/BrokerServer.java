package com.silveryark.gateway.broker;

import com.silveryark.rpc.RPCRequest;
import com.silveryark.rpc.gateway.OutboundMessage;
import com.silveryark.rpc.serializer.OutboundMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;

@Service
public class BrokerServer {

    private static final int TIMEOUT_FOR_TERMINATION_WORKER = 10;
    private static final int TIMEOUT_FOR_TERMINATION_JOB = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServer.class);

    @Value("${broker.connection}")
    private String connection;
    private ExecutorService workers = Executors.newSingleThreadExecutor();
    private final OutboundMessageSerializer outboundMessageSerializer;

    private ZContext context;
    private ZMQ.Socket publisher;

    private static BlockingQueue<Tuple2<Topic, OutboundMessage>> messageQueue = new LinkedBlockingQueue<>();

    @Autowired
    public BrokerServer(OutboundMessageSerializer outboundMessageSerializer) {
        this.outboundMessageSerializer = outboundMessageSerializer;
    }

    public void apply(Topic topic, OutboundMessage message){
        messageQueue.add(Tuples.of(topic, message));
    }

    //注册broker，启动event loop
    @PostConstruct
    protected void init() {
        context = new ZContext();
        publisher = context.createSocket(ZMQ.PUB);
        publisher.bind(connection);
        workers.submit(() -> {
            while(!Thread.currentThread().isInterrupted()){
                try {
                    Tuple2<Topic, OutboundMessage> packedMessage = messageQueue.take();
                    LOGGER.debug("send message {}", packedMessage.getT2());
                    try {
                        byte[] serializedMessage = outboundMessageSerializer.serialize(packedMessage.getT2());
                        publisher.sendMore(packedMessage.getT1().name);
                        publisher.send(serializedMessage);
                    } catch (IOException e) {
                        LOGGER.error("serialize outbound message {} error", packedMessage.getT2(), e);
                    }

                } catch (InterruptedException e) {
                    LOGGER.debug("Interrupted");
                    Thread.currentThread().interrupt();
                }

            }
        });
    }

    @PreDestroy
    protected void destroy() {
        //等待所有消息都消费光
        try {
            while(!messageQueue.isEmpty()){
                    Thread.sleep(TIMEOUT_FOR_TERMINATION_JOB);
            }
            //停掉发送者
            publisher.close();
            context.close();
            workers.shutdownNow();

            while (!workers.isTerminated()) {
                workers.awaitTermination(TIMEOUT_FOR_TERMINATION_WORKER, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when cleanup", e);
            Thread.currentThread().interrupt();
        }
    }

    public enum Topic{
        CMD("cmd"), MESSAGE("message");
        private String name;
        Topic(String name){
            this.name = name;
        }
    }
}
