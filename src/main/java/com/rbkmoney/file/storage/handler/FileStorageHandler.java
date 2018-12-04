package com.rbkmoney.file.storage.handler;

import com.rbkmoney.damsel.msgpack.Value;
import com.rbkmoney.file.storage.FileData;
import com.rbkmoney.file.storage.FileNotFound;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.file.storage.service.StorageService;
import com.rbkmoney.file.storage.service.exception.StorageException;
import com.rbkmoney.file.storage.service.exception.StorageFileNotFoundException;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

import static com.rbkmoney.file.storage.util.CheckerUtil.checkFileName;
import static com.rbkmoney.file.storage.util.CheckerUtil.checkString;

@RequiredArgsConstructor
@Slf4j
public class FileStorageHandler implements FileStorageSrv.Iface {

    private final StorageService storageService;

    @Override
    public FileData getFileData(String fileId) throws TException {
        try {
            log.info("Request getFileData fileId: {}", fileId);
            checkString(fileId, "Bad request parameter, fileId required and not empty arg");
            FileData fileData = storageService.getFileData(fileId);
            log.info("Response: fileData: {}", fileData);
            return fileData;
        } catch (StorageFileNotFoundException e) {
            log.error("Error when getFileData e: ", e);
            throw new FileNotFound();
        } catch (StorageException e) {
            log.error("Error when getFileData e: ", e);
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Error when getFileData e: ", e);
            throw new TException(e);
        }
    }

    @Override
    public NewFileResult createNewFile(String fileName, Map<String, Value> metadata, String expiresAt) throws TException {
        try {
            log.info("Request createNewFile fileName: {}, metadata: {}, expiresAt: {}", fileName, metadata, expiresAt);
            checkString(fileName, "Bad request parameter, fileName required and not empty arg");
            checkFileName(fileName, "Bad request parameter, enter the correct fileName");
            checkString(expiresAt, "Bad request parameter, expiresAt required and not empty arg");
            Instant instant = TypeUtil.stringToInstant(expiresAt);
            NewFileResult newFile = storageService.createNewFile(fileName, metadata, instant);
            log.info("Response: newFileResult: {}", newFile);
            return newFile;
        } catch (StorageFileNotFoundException e) {
            log.error("Error when createNewFile e: ", e);
            throw new FileNotFound();
        } catch (StorageException e) {
            log.error("Error when createNewFile e: ", e);
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Error when createNewFile e: ", e);
            throw new TException(e);
        }
    }

    @Override
    public String generateDownloadUrl(String fileId, String expiresAt) throws TException {
        try {
            log.info("Request generateDownloadUrl fileId: {}, expiresAt: {}", fileId, expiresAt);
            checkString(fileId, "Bad request parameter, fileId required and not empty arg");
            checkString(expiresAt, "Bad request parameter, expiresAt required and not empty arg");
            Instant instant = TypeUtil.stringToInstant(expiresAt);
            URL url = storageService.generateDownloadUrl(fileId, instant);
            log.info("Response: url: {}", url);
            return url.toString();
        } catch (StorageFileNotFoundException e) {
            log.error("Error when generateDownloadUrl e: ", e);
            throw new FileNotFound();
        } catch (StorageException e) {
            log.error("Error when generateDownloadUrl e: ", e);
            throw new WUnavailableResultException(e);
        } catch (Exception e) {
            log.error("Error when generateDownloadUrl e: ", e);
            throw new TException(e);
        }
    }
}
