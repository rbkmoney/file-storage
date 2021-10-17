package com.rbkmoney.file.storage.awssdks3v2;

import com.rbkmoney.file.storage.FileStorageTest;
import com.rbkmoney.testcontainers.annotations.ceph.CephTestcontainerSingleton;

@CephTestcontainerSingleton(
        properties = {"s3-sdk-v2.enabled=true", "s3-sdk-v2.region=us-east-1"},
        bucketName = "awssdks3v2")
public class WithCeph extends FileStorageTest {
}
