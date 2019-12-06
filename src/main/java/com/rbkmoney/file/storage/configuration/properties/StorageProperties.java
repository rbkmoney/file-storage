package com.rbkmoney.file.storage.configuration.properties;

import com.amazonaws.Protocol;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties("storage")
public class StorageProperties {

    private String endpoint;
    private String signingRegion;
    private String accessKey = "";
    private String secretKey = "";
    private Protocol clientProtocol;
    private Integer clientMaxErrorRetry;
    private String bucketName;

}
