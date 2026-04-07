package org.example.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "tarantool")
public class TarantoolProperties {
    private String host;
    private Integer port;
    private String crudUserName;
    private String crudUserPassword;
    private Integer batchSize;
}
