package com.silveryark.gateway.broker.connector;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Service
public class ZMQConnector implements Connector {

    @Value("${broker.connection}")
    private String connection;

    private ZContext context;
    private ZMQ.Socket publisher;

    @PostConstruct
    protected void init() {
        context = new ZContext();
        publisher = context.createSocket(ZMQ.PUB);
        publisher.bind(connection);
    }

    @Override
    public void close() throws IOException {
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
