package com.rbkmoney.file.storage.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageConfig {

    @Value("${storage.endpoint}")
    private String endpoint;

    @Value("${storage.signingRegion}")
    private String signingRegion;

    @Value("${storage.accessKey:}")
    private String accessKey;

    @Value("${storage.secretKey:}")
    private String secretKey;

    @Value("${storage.client.protocol:HTTP}")
    private Protocol protocol;

    @Value("${storage.client.maxErrorRetry}")
    private int maxErrorRetry;

    @Bean
    public AmazonS3 storageClient(AWSCredentialsProviderChain credentialsProviderChain, ClientConfiguration clientConfiguration) {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProviderChain)
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, signingRegion)
                )
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    @Bean
    public AWSCredentialsProviderChain credentialsProviderChain() {
        return new AWSCredentialsProviderChain(
                new EnvironmentVariableCredentialsProvider(),
                new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                                accessKey,
                                secretKey
                        )
                )
        );
    }

    @Bean
    public ClientConfiguration clientConfiguration() {
        return new ClientConfiguration()
                .withProtocol(protocol)
                .withSignerOverride("S3SignerType")
                .withMaxErrorRetry(maxErrorRetry);
    }

    @Bean
    public TransferManager transferManager(AmazonS3 s3Client) {
        return TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
    }
}
