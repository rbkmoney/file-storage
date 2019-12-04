package com.rbkmoney.file.storage.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.configuration.properties.StorageProperties;
import com.rbkmoney.file.storage.msgpack.Value;
import com.rbkmoney.file.storage.service.exception.StorageException;
import com.rbkmoney.file.storage.service.exception.StorageFileNotFoundException;
import com.rbkmoney.file.storage.service.exception.StorageWaitingUploadException;
import com.rbkmoney.file.storage.util.DamselUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
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

    private static final String FILE_DATA_ID = "x-rbkmoney-file-data-id";
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
        if (!s3Client.doesBucketExistV2(bucketName)) {
            s3Client.createBucket(bucketName);
        }
    }

    @Override
    public NewFileResult createNewFile(Map<String, Value> metadata, Instant expirationTime) {
        log.info("Trying to create new file, bucketId='{}'", bucketName);

        InputStream emptyContent = getEmptyContent();
        String fileDataId = createId();
        String fileId = createId();
        FileDto fileDto = new FileDto(
                fileDataId,
                fileId,
                Instant.now().toString(),
                metadata
        );

        try {
            // в хранилище записывается неизменяемый фейковый файл с метаданными,
            // в котором находится ссылка на реальный файл
            log.info("Upload fake metadata file, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);
            uploadRequest(fileDataId, fileDto, emptyContent);

            // генерируем ссылку на выгрузку файла в хранилище напрямую в цеф по ключу fileId
            URL uploadUrl = generateUploadUrl(fileId, expirationTime);

            log.info("New empty real file have been successfully created, fileDataId='{}', bucketId='{}'", fileDataId, bucketName);

            return new NewFileResult(fileDataId, uploadUrl.toString());
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to create new file, fileDataId='%s', bucketId='%s'",
                            fileDataId,
                            bucketName
                    ),
                    ex
            );
        }
    }

    @Override
    public URL generateDownloadUrl(String fileDataId, Instant expirationTime) {
        String fileId = getFileDto(fileDataId).getFileId();
        // генерируем ссылку на загрузку файла из хранилища напрямую в цеф по ключу fileId
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.GET);
    }

    @Override
    public FileData getFileData(String fileDataId) {
        FileDto fileDto = getFileDto(fileDataId);

        S3Object object = getS3Object(fileDto.getFileId());

        String fileName = extractFileName(object);

        return new FileData(fileDto.getFileDataId(), fileName, fileDto.getCreatedAt(), fileDto.getMetadata());
    }

    @PreDestroy
    public void terminate() {
        transferManager.shutdownNow(true);
    }

    private S3Object getS3Object(String id) {
        try {
            log.info("Trying to get fake metadata file, id='{}', bucketId='{}'", id, bucketName);

            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, id));

            checkNotNull(object, id, "File");

            log.info("Fake metadata file successfully got, id='{}', bucketId='{}'", id, bucketName);

            return object;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format("Failed to get fake metadata file, fileDataId='%s', bucketId='%s'", id, bucketName),
                    ex
            );
        }
    }

    private void checkRealFileStatus(S3Object s3Object) {
        log.info("Check real file expiration and uploaded status by fake metadata file: ETag='{}'", s3Object.getObjectMetadata().getETag());
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
        log.info("Trying to extract real file metadata by fake metadata file: ETag='{}'", objectMetadata.getETag());

        String id = getUserMetadataParameter(objectMetadata, FILE_DATA_ID);
        String fileId = getFileIdFromObjectMetadata(objectMetadata);
        String createdAt = getUserMetadataParameter(objectMetadata, CREATED_AT);
        Map<String, Value> metadata = objectMetadata.getUserMetadata().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(METADATA) && entry.getValue() != null)
                .collect(
                        Collectors.toMap(
                                o -> o.getKey().substring(METADATA.length()),
                                o -> DamselUtil.fromJson(o.getValue(), Value.class)
                        )
                );

        log.info("Real file metadata have been successfully extracted, id='{}', bucketId='{}'", id, bucketName);

        return new FileDto(id, fileId, createdAt, metadata);
    }

    private URL generatePresignedUrl(String fileId, Instant expirationTime, HttpMethod httpMethod) {
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
                    "Presigned url for real file have been successfully generated, fileId='{}', bucketId='{}', expirationTime='{}', httpMethod='{}'",
                    fileId,
                    bucketName,
                    expirationTime,
                    httpMethod
            );
            return url;
        } catch (AmazonClientException ex) {
            throw new StorageException(
                    String.format(
                            "Failed to generate presigned url for real file, fileId='%s', bucketId='%s', expirationTime='%s', httpMethod='%s'",
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

    private void uploadRequest(String fileDataId, FileDto fileDto, InputStream inputStream) {
        PutObjectRequest putObjectRequest = createS3Request(fileDataId, fileDto, inputStream);

        Upload upload = transferManager.upload(putObjectRequest);

        try {
            upload.waitForUploadResult();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageWaitingUploadException(
                    String.format("Thread is interrupted while waiting for the upload to complete, file=%s", fileDto.toString())
            );
        }
    }

    private PutObjectRequest createS3Request(String fileDataId, FileDto fileDto, InputStream inputStream) {
        return new PutObjectRequest(
                bucketName,
                fileDataId,
                inputStream,
                createObjectMetadata(fileDto)
        );
    }

    private ObjectMetadata createObjectMetadata(FileDto fileDto) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata(FILE_DATA_ID, fileDto.getFileDataId());
        objectMetadata.addUserMetadata(FILE_ID, fileDto.getFileId());
        objectMetadata.addUserMetadata(CREATED_AT, fileDto.getCreatedAt());
        fileDto.getMetadata().forEach(
                (key, value) -> objectMetadata.addUserMetadata(METADATA + key, DamselUtil.toJsonString(value))
        );
        return objectMetadata;
    }

    private FileDto getFileDto(String id) {
        // извлечение фейкового файла с метаданными о реальном файле
        S3Object s3Object = getS3Object(id);

        // функция возвращает метаданные только в случае, если реальный файл был уже выгружен в хранилище
        checkRealFileStatus(s3Object);

        return getFileDtoByFakeFile(s3Object.getObjectMetadata());
    }

    private URL generateUploadUrl(String fileId, Instant expirationTime) {
        return generatePresignedUrl(fileId, expirationTime, HttpMethod.PUT);
    }

    private String getFileIdFromObjectMetadata(ObjectMetadata objectMetadata) {
        return getUserMetadataParameter(objectMetadata, FILE_ID);
    }

    private String getUserMetadataParameter(ObjectMetadata objectMetadata, String key) {
        return Optional.ofNullable(objectMetadata.getUserMetaDataOf(key))
                .orElseThrow(() -> new StorageException("Failed to extract metadata parameter, " + key + " is null"));
    }

    private String createId() {
        return UUID.randomUUID().toString();
    }

    private void checkNotNull(Object object, String fileId, String objectType) {
        if (Objects.isNull(object)) {
            throw new StorageFileNotFoundException(String.format(objectType + " is null, fileId='%s', bucketId='%s'", fileId, bucketName));
        }
    }

    private String extractFileName(S3Object object) {
        String contentDisposition = object.getObjectMetadata().getContentDisposition();
        int fileNameIndex = contentDisposition.lastIndexOf(FILENAME_PARAM) + FILENAME_PARAM.length();
        return contentDisposition.substring(fileNameIndex);
    }

    @RequiredArgsConstructor
    @Getter
    @ToString
    private class FileDto {

        private final String fileDataId;
        private final String fileId;
        private final String createdAt;
        private final Map<String, Value> metadata;

    }
}
