package com.rbkmoney.file.storage.configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.rbkmoney.file.storage.configuration.properties.StorageProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(StorageProperties.class)
public class AmazonS3ClientConfiguration {

    private final StorageProperties storageProperties;

    @Bean
    public AmazonS3 storageClient(
            AWSCredentialsProviderChain credentialsProviderChain,
            ClientConfiguration clientConfiguration) {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProviderChain)
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                storageProperties.getEndpoint(),
                                storageProperties.getSigningRegion()
                        )
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
                                storageProperties.getAccessKey(),
                                storageProperties.getSecretKey()
                        )
                )
        );
    }

    @Bean
    public ClientConfiguration clientConfiguration() {
        return new ClientConfiguration()
                .withProtocol(storageProperties.getClientProtocol())
                .withSignerOverride("AWSS3V4SignerType")
                .withMaxErrorRetry(storageProperties.getClientMaxErrorRetry());
    }

    @Bean
    public TransferManager transferManager(AmazonS3 s3Client) {
        return TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
    }
}
