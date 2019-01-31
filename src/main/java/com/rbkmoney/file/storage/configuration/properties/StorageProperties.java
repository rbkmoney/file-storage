package com.rbkmoney.file.storage.configuration.properties;


import com.amazonaws.Protocol;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("storage")
@Getter
@Setter
public class StorageProperties {

    private String endpoint;
    private String signingRegion;
    private String accessKey = "";
    private String secretKey = "";
    private Protocol clientProtocol;
    private Integer clientMaxErrorRetry;
    private String bucketName;

}
