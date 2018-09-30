package com.silveryark.gateway.broker.connector;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
public class ZMQConnector implements Connector {

    private static ZContext context = new ZContext();
    private static ZMQ.Socket publisher = context.createSocket(ZMQ.PUB);
    @Value("${broker.connection}")
    private String connection;

    protected ZMQConnector() {
        
    }

    @PostConstruct
    protected void init() {
        publisher.bind(connection);
    }

    @PreDestroy
    protected void destroy() {
        publisher.close();
        context.close();
    }

    @Override
    public void sendMore(byte[] message) {
        publisher.sendMore(message);
    }

    @Override
    public void send(byte[] message) {
        publisher.send(message);
    }
}
