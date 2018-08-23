package com.silveryark.gateway.broker.controller;

import com.silveryark.gateway.broker.BrokerServer;
import com.silveryark.rpc.GenericResponse;
import com.silveryark.rpc.RPCResponse;
import com.silveryark.rpc.gateway.OutboundMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class MessageController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageController.class);

    private final BrokerServer brokerServer;

    @Autowired
    public MessageController(BrokerServer brokerServer) {
        this.brokerServer = brokerServer;
    }

    @PostMapping("/cmd")
    public Mono<GenericResponse> cmd(@RequestBody OutboundMessage request) {
        LOGGER.debug("received cmd message {}", request);
        brokerServer.apply(BrokerServer.Topic.CMD, request);
        return Mono.just(new GenericResponse(request.getRequestId(), RPCResponse.STATUS.OK, Boolean.TRUE));
    }

    @PostMapping("/message")
    public Mono<GenericResponse> message(@RequestBody OutboundMessage request) {
        LOGGER.debug("received normal message {}", request);
        brokerServer.apply(BrokerServer.Topic.MESSAGE, request);
        return Mono.just(new GenericResponse(request.getRequestId(), RPCResponse.STATUS.OK, Boolean.TRUE));
    }
}
