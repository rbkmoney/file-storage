package com.rbkmoney.file.storage.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.rbkmoney.damsel.msgpack.Value;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.configuration.properties.StorageProperties;
import com.rbkmoney.file.storage.service.exception.StorageException;
import com.rbkmoney.file.storage.service.exception.StorageFileNotFoundException;
import com.rbkmoney.file.storage.util.DamselUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class AmazonS3StorageService implements StorageService {

    private static final String ID = "x-rbkmoney-id";
    private static final String FILE_ID = "x-rbkmoney-file-id";
    private static final String CREATED_AT = "x-rbkmoney-created-at";
    private static final String METADATA = "x-rbkmoney-metadata-";
    private static final String FILENAME_PARAM = "filename=";

    private final TransferManager transferManager;
    private final AmazonS3 s3Client;
    private final StorageProperties storageProperties;
    private String bucketName;

    @PostConstruct
    public void init() {
        this.bucketName = storageProperties.getBucketName();
        if (!s3Client.doesBucketExist(bucketName)) {
            log.info("Create bucket in file storage, bucketId='{}'", bucketName);
            s3Client.createBucket(bucketName);
        }
    }

    @Override
    public NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime) throws StorageException {
        log.info("Trying to create new file to storage, bucketId='{}'", bucketName);

        InputStream emptyContent = getEmptyContent();
        String id = createId();
        String fileId = createId();
        FileDto fileDto = new FileDto(
                id,
                fileId,
                Instant.now().toString(),
                metadata
        );

        try {
            // в хранилище записывается неизменяемый фейковый файл с метаданными,
            // в котором находится ссылка на реальный файл
            uploadRequest(id, fileDto, emptyContent);

            // генерируем ссылку на выгрузку файла в хранилище напрямую в цеф по ключу fileId
            URL uploadUrl = generateUploadUrl(fileId, expirationTime);

            log.info("File have been successfully created, id='{}', bucketId='{}'", id, bucketName);

            return new NewFileResult(id, uploadUrl.toString());
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to create new file to storage, id='%s', bucketId='%s'",
                            id,
                            bucketName
                    ),
                    ex
            );
        }
    }

    @Override
    public URL generateDownloadUrl(String id, Instant expirationTime) throws StorageException {
        String fileId = getFileDto(id).getFileId();
        // генерируем ссылку на загрузку файла из хранилища напрямую в цеф по ключу fileId
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.GET);
    }

    @Override
    public FileData getFileData(String id) throws StorageException {
        FileDto fileDto = getFileDto(id);

        S3Object object = getS3Object(fileDto.getFileId());

        String fileName;
        try {
            String contentDisposition = object.getObjectMetadata().getContentDisposition();
            int fileNameIndex = contentDisposition.lastIndexOf(FILENAME_PARAM) + FILENAME_PARAM.length();

            fileName = contentDisposition.substring(fileNameIndex);
        } catch (NullPointerException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to extract fileName, id='%s', bucketId='%s'",
                            id,
                            bucketName
                    ),
                    ex
            );
        }

        return new FileData(fileDto.getId(), fileName, fileDto.getCreatedAt(), fileDto.getMetadata());
    }

    @PreDestroy
    public void terminate() {
        transferManager.shutdownNow(true);
    }

    private S3Object getS3Object(String id) throws StorageException {
        try {
            log.info(
                    "Trying to get file from storage, id='{}', bucketId='{}'",
                    id,
                    bucketName
            );
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, id);
            S3Object object = s3Client.getObject(getObjectRequest);
            checkNotNull(object, id, "File");
            log.info(
                    "File have been successfully got from storage, id='{}', bucketId='{}'",
                    id,
                    bucketName
            );
            return object;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to get file from storage, fileDataId='%s', bucketId='%s'",
                            id,
                            bucketName
                    ),
                    ex
            );
        }
    }

    private void checkRealFileStatus(S3Object s3Object) throws StorageFileNotFoundException {
        log.info("Check real file expiration and uploaded status: ETag='{}'", s3Object.getObjectMetadata().getETag());
        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

        String fileId = getFileIdFromObjectMetadata(objectMetadata);
        if (s3Client.doesObjectExist(bucketName, fileId)) {
            log.info("Real file was uploaded: ETag='{}'", s3Object.getObjectMetadata().getETag());
            return;
        }

        // если файл не соотвествует условиям, блокируем доступ к нему
        throw new StorageFileNotFoundException(String.format("Real file not found: id='%s', bucketId='%s', create a new file", s3Object.getKey(), bucketName));
    }

    private FileDto getFileDtoByFakeFile(ObjectMetadata objectMetadata) {
        log.info("Trying to extract real file metadata by fake file from storage: ETag='{}'", objectMetadata.getETag());
        String id = getUserMetadataParameter(objectMetadata, ID);
        String fileId = getFileIdFromObjectMetadata(objectMetadata);
        String createdAt = getUserMetadataParameter(objectMetadata, CREATED_AT);
        Map<String, com.rbkmoney.damsel.msgpack.Value> metadata = objectMetadata.getUserMetadata().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(METADATA) && entry.getValue() != null)
                .collect(
                        Collectors.toMap(
                                o -> o.getKey().substring(METADATA.length()),
                                o -> DamselUtil.fromJson(o.getValue(), com.rbkmoney.damsel.msgpack.Value.class)
                        )
                );
        log.info(
                "Real file metadata have been successfully extracted by fake file from storage, id='{}', bucketId='{}'",
                id,
                bucketName
        );
        return new FileDto(id, fileId, createdAt, metadata);
    }

    private URL generatePresignedUrl(String fileId, Instant expirationTime, HttpMethod httpMethod) throws StorageException {
        try {
            log.info(
                    "Trying to generate presigned url for real file, fileId='{}', bucketId='{}', expirationTime='{}', httpMethod='{}'",
                    fileId,
                    bucketName,
                    expirationTime,
                    httpMethod
            );

            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, fileId)
                    .withMethod(httpMethod)
                    .withExpiration(Date.from(expirationTime));
            URL url = s3Client.generatePresignedUrl(request);
            checkNotNull(url, fileId, "Presigned url");
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

    private ByteArrayInputStream getEmptyContent() {
        return new ByteArrayInputStream(new byte[0]);
    }

    private void uploadRequest(String id, FileDto fileDto, InputStream inputStream) throws AmazonClientException {
        PutObjectRequest putObjectRequest = createS3Request(id, fileDto, inputStream);

        Upload upload = transferManager.upload(putObjectRequest);
        try {
            upload.waitForUploadResult();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private PutObjectRequest createS3Request(String id, FileDto fileDto, InputStream inputStream) {
        return new PutObjectRequest(
                bucketName,
                id,
                inputStream,
                createObjectMetadata(fileDto)
        );
    }

    private ObjectMetadata createObjectMetadata(FileDto fileDto) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata(ID, fileDto.getId());
        objectMetadata.addUserMetadata(FILE_ID, fileDto.getFileId());
        objectMetadata.addUserMetadata(CREATED_AT, fileDto.getCreatedAt());
        fileDto.getMetadata().forEach(
                (key, value) -> objectMetadata.addUserMetadata(METADATA + key, DamselUtil.toJsonString(value))
        );
        return objectMetadata;
    }

    private FileDto getFileDto(String id) throws StorageException {
        // извлечение фейкового файла с метаданными о реальном файле
        S3Object s3Object = getS3Object(id);
        // функция возвращает метаданные только в случае, если реальный файл был уже выгружен в хранилище
        checkRealFileStatus(s3Object);
        return getFileDtoByFakeFile(s3Object.getObjectMetadata());
    }

    private URL generateUploadUrl(String fileId, Instant expirationTime) throws StorageException {
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.PUT);
    }

    private String getFileIdFromObjectMetadata(ObjectMetadata objectMetadata) {
        return getUserMetadataParameter(objectMetadata, FILE_ID);
    }

    private String getUserMetadataParameter(ObjectMetadata objectMetadata, String key) throws StorageException {
        return Optional.ofNullable(objectMetadata.getUserMetaDataOf(key))
                .orElseThrow(() -> new StorageException("Failed to extract user metadata parameter, " + key + " is null"));
    }

    private String createId() {
        return UUID.randomUUID().toString();
    }

    private void checkNotNull(Object object, String fileId, String objectType) throws StorageFileNotFoundException {
        if (Objects.isNull(object)) {
            throw new StorageFileNotFoundException(String.format(objectType + " is null, fileId='%s', bucketId='%s'", fileId, bucketName));
        }
    }

    @RequiredArgsConstructor
    @Getter
    private class FileDto {

        private final String id;
        private final String fileId;
        private final String createdAt;
        private final Map<String, Value> metadata;

    }
}