package com.silveryark.gateway.broker;

import com.silveryark.gateway.broker.connector.Connector;
import com.silveryark.rpc.gateway.OutboundMessage;
import com.silveryark.rpc.serializer.OutboundMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.*;

@Service
public class BrokerServer {

    private static final int TIMEOUT_FOR_TERMINATION_WORKER = 10;
    private static final int TIMEOUT_FOR_TERMINATION_JOB = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServer.class);
    private final OutboundMessageSerializer outboundMessageSerializer;
    private final Connector connector;
    private BlockingQueue<Tuple2<Topic, OutboundMessage>> messageQueue = new LinkedBlockingQueue<>();
    private ExecutorService workers = Executors.newSingleThreadExecutor();

    @Autowired
    public BrokerServer(OutboundMessageSerializer outboundMessageSerializer, Connector connector) {
        this.outboundMessageSerializer = outboundMessageSerializer;
        this.connector = connector;
        //注册broker，启动event loop，之所以不放到PostConstruct里面是因为在unit test的时候 global的 对象不会被调用 construct
        //只有每个test时创建的Service会PostConstruct
        workers.submit(() -> {
            LOGGER.info("BrokerServer worker initialized and running...");
            while (!Thread.currentThread().isInterrupted()) {
                Tuple2<Topic, OutboundMessage> packedMessage = null;
                try {
                    packedMessage = messageQueue.take();
                    LOGGER.debug("send message {}", packedMessage.getT2());
                    LOGGER.debug("send message with connector: {}, and serializer: {}", connector,
                            outboundMessageSerializer);
                    byte[] serializedMessage = outboundMessageSerializer.serialize(packedMessage.getT2());
                    connector.sendMore(packedMessage.getT1().name().getBytes());
                    connector.send(serializedMessage);
                    LOGGER.debug("serialized message {} sent", serializedMessage);
                } catch (InterruptedException e) {
                    LOGGER.debug("Interrupted");
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    if (packedMessage != null) {
                        LOGGER.error("serialize outbound message {} error", packedMessage.getT2(), e);
                    }
                } catch (Exception e) {
                    LOGGER.error("Unknown exception.", e);
                }
            }
        });
    }

    public void apply(Topic topic, OutboundMessage message) {
        messageQueue.add(Tuples.of(topic, message));
    }

    @PreDestroy
    protected void destroy() {
        //等待所有消息都消费光
        try {
            while (!messageQueue.isEmpty()) {
                Thread.sleep(TIMEOUT_FOR_TERMINATION_JOB);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when cleanup", e);
            Thread.currentThread().interrupt();
        }
        //停掉发送者
        try {
            connector.close();
        } catch (IOException e) {
            LOGGER.error("IOException when close connector", e);
        }
        workers.shutdownNow();
        try {
            while (!workers.isTerminated()) {
                workers.awaitTermination(TIMEOUT_FOR_TERMINATION_WORKER, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when cleanup", e);
            Thread.currentThread().interrupt();
        }
    }

    public enum Topic {
        CMD, MESSAGE;
    }
}
