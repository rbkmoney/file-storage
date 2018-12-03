package com.rbkmoney.file.storage.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.contorller.UploadFileController;
import com.rbkmoney.file.storage.service.exception.StorageException;
import com.rbkmoney.file.storage.util.DamselUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AmazonS3StorageService implements StorageService {

    private static final String EXPIRATION_TIME = "x-rbkmoney-file-expiration-time";
    private static final String FILE_UPLOADED = "x-rbkmoney-file-uploaded";
    private static final String FILEDATA_FILE_ID = "x-rbkmoney-filedata-file-id";
    private static final String FILEDATA_FILE_NAME = "x-rbkmoney-filedata-file-name";
    private static final String FILEDATA_CREATED_AT = "x-rbkmoney-filedata-created-at";
    private static final String FILEDATA_MD_5 = "x-rbkmoney-filedata-md5";
    private static final String FILEDATA_METADATA = "x-rbkmoney-filedata-metadata-";

    private final TransferManager transferManager;
    private final AmazonS3 s3Client;
    private final String bucketName;

    @Autowired
    public AmazonS3StorageService(TransferManager transferManager, @Value("${storage.bucketName}") String bucketName) {
        this.transferManager = transferManager;
        this.s3Client = transferManager.getAmazonS3Client();
        this.bucketName = bucketName;
    }

    @PostConstruct
    public void init() {
        if (!s3Client.doesBucketExist(bucketName)) {
            log.info("Create bucket in file storage, bucketId='{}'", bucketName);
            s3Client.createBucket(bucketName);
        }
    }

    @Override
    public FileData getFileData(String fileId) throws StorageException, FileNotFoundException {
        S3Object s3Object = getS3Object(fileId);
        checkFileStatus(s3Object);
        return extractFileData(s3Object.getObjectMetadata());
    }

    @Override
    public NewFileResult createNewFile(String fileName, Map<String, com.rbkmoney.damsel.msgpack.Value> metadata, Instant expirationTime) throws StorageException {
        log.info("Trying to create new file to storage, filename='{}', bucketId='{}'", fileName, bucketName);

        try {
            // в хранилище сохраняется пустой файл
            InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

            String fileId = getFileId();
            String createdAt = Instant.now().toString();

            FileData fileData = new FileData(
                    fileId,
                    fileName,
                    createdAt,
                    //todo
                    "",
                    metadata
            );

            writeFileToStorage(fileData, emptyContent, expirationTime);

            URL uploadUrl = createUploadUrl(fileId);

            log.info(
                    "File have been successfully created, fileId='{}', bucketId='{}', filename='{}', md5='{}'",
                    fileId,
                    bucketName,
                    fileName,
                    //todo
                    ""
            );

            return new NewFileResult(uploadUrl.toString(), fileData);
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to create new file to storage, filename='%s', bucketId='%s'",
                            fileName,
                            bucketName
                    ),
                    ex
            );
        }
    }

    @Override
    public URL generateDownloadUrl(String fileId, Instant expirationTime) throws StorageException, FileNotFoundException {
        checkFileStatus(getS3Object(fileId));
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.GET);
    }

    @Override
    public void uploadFile(String fileId, InputStream inputStream) throws StorageException, IOException {
        log.info("Trying to upload file to storage, filename='{}', bucketId='{}'", fileId, bucketName);

        try {

            //todo
            Path testFile = Files.createTempFile("", "test_file");
            Files.write(testFile, "Test".getBytes());

            S3Object object = getS3Object(fileId);

            checkFileStatus(object);

            object.getObjectMetadata().addUserMetadata(FILE_UPLOADED, "true");
/*
            //todo DEBUG
            try {
                //            InputStream inputStream = file.getInputStream();
                Path testActualFile = Files.createTempFile("", "test_actual_file");
                Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("!!!!!!!");
                for (String readAllLine : Files.readAllLines(testActualFile)) {
                    System.out.println(readAllLine);
                }
           testActualFile = Files.createTempFile("", "test_actual_file");
            Files.copy(inputStream, testActualFile, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("!!!!!!!");
            for (String readAllLine : Files.readAllLines(testActualFile)) {
                System.out.println(readAllLine);
            }
            } catch (Exception e) {
            }
            //todo
*/

            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, fileId, testFile.toFile());
            putObjectRequest.setMetadata(object.getObjectMetadata());
            s3Client.putObject(putObjectRequest);

/*
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, fileId, new S3ObjectInputStream(
                    new FileInputStream(testFile.toFile()),null
            ),object.getObjectMetadata());
            s3Client.putObject(putObjectRequest);
*/
/*
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, fileId, new FileInputStream(testFile.toFile()),object.getObjectMetadata());
            s3Client.putObject(putObjectRequest);
*/

/* todo
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, fileId, inputStream, object.getObjectMetadata());
            s3Client.putObject(putObjectRequest);
*/
            log.info(
                    "File have been successfully uploaded, fileId='{}', bucketId='{}'",
                    fileId,
                    bucketName
            );

            //todo
     /*       try {
                S3Object object1 = s3Client.getObject(bucketName, fileId);
                S3ObjectInputStream objectContent = object1.getObjectContent();
                Path testActualFile = Files.createTempFile("", "test_actual_file");
                Files.copy(objectContent, testActualFile, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("!!!!!!!");
                for (String readAllLine : Files.readAllLines(testActualFile)) {
                    System.out.println(readAllLine);
                }
            } catch (Exception e) {
            }
*/
            //todo
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to to upload file to storage, filename='%s', bucketId='%s'",
                            fileId,
                            bucketName
                    ),
                    ex
            );
        }
    }

    @PreDestroy
    public void terminate() {
        transferManager.shutdownNow(true);
    }

    private S3Object getS3Object(String fileId) throws StorageException, FileNotFoundException {
        try {
            log.info(
                    "Trying to get file from storage, fileId='{}', bucketId='{}'",
                    fileId,
                    bucketName
            );
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileId);
            S3Object object = s3Client.getObject(getObjectRequest);
            checkNullable(object, fileId, "File");
            log.info(
                    "File have been successfully got from storage, fileId='{}', bucketId='{}'",
                    fileId,
                    bucketName
            );
            return object;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to get file from storage, fileId='%s', bucketId='%s'",
                            fileId,
                            bucketName
                    ),
                    ex
            );
        }
    }

    private void checkFileStatus(S3Object s3Object) throws StorageException {
        log.info("Check file expiration and uploaded status: ETag='{}'", s3Object.getObjectMetadata().getETag());
        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

        Boolean isUploaded = getBooleanFromObjectMetadata(objectMetadata);
        if (isUploaded) {
            log.info("File was uploaded: ETag='{}'", s3Object.getObjectMetadata().getETag());
            return;
        }

        Date expirationTime = getDateFromObjectMetadata(objectMetadata);
        Date time = new Date();
        if (time.getTime() < expirationTime.getTime()) {
            log.info("File was uploaded: ETag='{}'", s3Object.getObjectMetadata().getETag());
            return;
        }

        // если файл не соотвествует условиям, блокируем доступ к нему
        throw new StorageException(String.format("File access error: fileId='%s', bucketId='%s', create a new file", s3Object.getKey(), bucketName));
    }

    private FileData extractFileData(ObjectMetadata objectMetadata) {
        log.info("Trying to extract metadata from storage: ETag='{}'", objectMetadata.getETag());
        String fileId = getUserMetadataParameter(objectMetadata, FILEDATA_FILE_ID);
        String fileName = getUserMetadataParameter(objectMetadata, FILEDATA_FILE_NAME);
        String createdAt = getUserMetadataParameter(objectMetadata, FILEDATA_CREATED_AT);
        String md5 = getUserMetadataParameter(objectMetadata, FILEDATA_MD_5);

        Map<String, com.rbkmoney.damsel.msgpack.Value> metadata = objectMetadata.getUserMetadata().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FILEDATA_METADATA) && entry.getValue() != null)
                .collect(
                        Collectors.toMap(
                                o -> o.getKey().substring(FILEDATA_METADATA.length()),
                                o -> DamselUtil.fromJson(o.getValue(), com.rbkmoney.damsel.msgpack.Value.class)
                        )
                );
        log.info(
                "Metadata have been successfully extracted from storage, fileId='{}', bucketId='{}'",
                fileId,
                bucketName
        );
        return new FileData(fileId, fileName, createdAt, md5, metadata);
    }

    private URL generatePresignedUrl(String fileId, Instant expirationTime, HttpMethod httpMethod) throws StorageException, FileNotFoundException {
        try {
            log.info(
                    "Trying to generate presigned url, fileId='{}', bucketId='{}', expirationTime='{}', httpMethod='{}'",
                    fileId,
                    bucketName,
                    expirationTime,
                    httpMethod
            );

            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, fileId)
                    .withMethod(httpMethod)
                    .withExpiration(Date.from(expirationTime));
            URL url = s3Client.generatePresignedUrl(request);
            checkNullable(url, fileId, "Presigned url");
            log.info(
                    "Presigned url have been successfully generated, url='{}', fileId='{}', bucketId='{}', expirationTime='{}', httpMethod='{}'",
                    url,
                    fileId,
                    bucketName,
                    expirationTime,
                    httpMethod
            );
            return url;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to generate presigned url, fileId='%s', bucketId='%s', expirationTime='%s', httpMethod='%s'",
                            fileId,
                            bucketName,
                            expirationTime,
                            httpMethod
                    ),
                    ex
            );
        }
    }

    private void writeFileToStorage(FileData fileData, InputStream inputStream, Instant expirationTime) throws AmazonClientException {
        PutObjectRequest request = createS3Request(fileData, inputStream, expirationTime);
        s3Client.putObject(request);
    }

    private PutObjectRequest createS3Request(FileData fileData, InputStream inputStream, Instant expirationTime) {
        return new PutObjectRequest(
                bucketName,
                fileData.getFileId(),
                inputStream,
                createObjectMetadata(fileData, expirationTime)
        );
    }

    private ObjectMetadata createObjectMetadata(FileData fileData, Instant expirationTime) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentDisposition("attachment;filename=" + fileData.getFileName());
        // file parameters
        objectMetadata.addUserMetadata(EXPIRATION_TIME, expirationTime.toString());
        objectMetadata.addUserMetadata(FILE_UPLOADED, "false");
        // filedata parameters
        objectMetadata.addUserMetadata(FILEDATA_FILE_ID, fileData.getFileId());
        objectMetadata.addUserMetadata(FILEDATA_FILE_NAME, fileData.getFileName());
        objectMetadata.addUserMetadata(FILEDATA_CREATED_AT, fileData.getCreatedAt());
        objectMetadata.addUserMetadata(FILEDATA_MD_5, fileData.getMd5());
        fileData.getMetadata().forEach(
                (key, value) -> objectMetadata.addUserMetadata(FILEDATA_METADATA + key, DamselUtil.toJsonString(value))
        );
        return objectMetadata;
    }

    private URL createUploadUrl(String fileId) {
        try {
            return MvcUriComponentsBuilder.fromMethodName(
                    UploadFileController.class,
                    "handleFileUpload",
                    fileId,
                    null
            )
                    .buildAndExpand()
                    .encode()
                    .toUri()
                    .toURL();
        } catch (MalformedURLException e) {
            throw new StorageException(
                    String.format(
                            "Exception createUploadUrl: fileId='%s', bucketId='%s', create a new file",
                            fileId,
                            bucketName
                    ),
                    e);
        }
    }

    private Boolean getBooleanFromObjectMetadata(ObjectMetadata objectMetadata) {
        String isUploadedString = getUserMetadataParameter(objectMetadata, FILE_UPLOADED);
        return Boolean.valueOf(isUploadedString);
    }

    private Date getDateFromObjectMetadata(ObjectMetadata objectMetadata) throws StorageException {
        String expirationTime = getUserMetadataParameter(objectMetadata, EXPIRATION_TIME);
        return Date.from(TypeUtil.stringToInstant(expirationTime));
    }

    private String getUserMetadataParameter(ObjectMetadata objectMetadata, String key) throws StorageException {
        return Optional.ofNullable(objectMetadata.getUserMetaDataOf(key))
                .orElseThrow(() -> new StorageException("Failed to extract user metadata parameter, " + key + " is null"));
    }

    private String getFileId() {
        String fileId;
        do {
            fileId = UUID.randomUUID().toString();
        } while (s3Client.doesObjectExist(bucketName, fileId));
        return fileId;
    }

    private void checkNullable(Object object, String fileId, String objectType) throws FileNotFoundException {
        if (Objects.isNull(object)) {
            throw new FileNotFoundException(String.format(objectType + " is null, fileId='%s', bucketId='%s'", fileId, bucketName));
        }
    }
}