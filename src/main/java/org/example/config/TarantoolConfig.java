package org.example.config;

import io.tarantool.client.TarantoolClient;
import io.tarantool.client.factory.TarantoolBoxClientBuilder;
import lombok.RequiredArgsConstructor;
import org.example.properties.TarantoolProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class TarantoolConfig {
    private final TarantoolProperties properties;
    @Bean
    public TarantoolClient getClient() throws Exception {
        var builder = new TarantoolBoxClientBuilder();
        return builder.withHost(properties.getHost())
                .withPort(properties.getPort())
                .withUser(properties.getCrudUserName())
                .withPassword(properties.getCrudUserPassword())
                .build();
    }

}
