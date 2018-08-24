package com.silveryark.gateway.broker.connector;

public interface Connector {
    void sendMore(byte[] message);

    void send(byte[] message);
}
