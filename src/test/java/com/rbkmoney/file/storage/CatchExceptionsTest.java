package com.rbkmoney.file.storage;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.rbkmoney.woody.api.flow.error.WErrorType;
import com.rbkmoney.woody.api.flow.error.WRuntimeException;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CatchExceptionsTest {

    @LocalServerPort
    protected int port;

    @MockBean
    private TransferManager transferManager;

    @MockBean
    private AmazonS3 s3Client;

    @Test
    public void shouldResourceUnavailable() throws URISyntaxException, TException {
        FileStorageSrv.Iface fileStorageCli = new THSpawnClientBuilder()
                .withAddress(new URI("http://localhost:" + port + "/file_storage"))
                .withNetworkTimeout(555000)
                .build(FileStorageSrv.Iface.class);

        Mockito.when(s3Client.getObject(Mockito.any())).thenThrow(SdkClientException.class);

        try {
            fileStorageCli.getFileData(UUID.randomUUID().toString());
        } catch (WRuntimeException e) {
            Assert.assertEquals(WErrorType.UNAVAILABLE_RESULT, e.getErrorDefinition().getErrorType());
        }
    }
}
