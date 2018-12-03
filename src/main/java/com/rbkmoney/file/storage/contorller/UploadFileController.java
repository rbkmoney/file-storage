package com.rbkmoney.file.storage.contorller;

import com.rbkmoney.file.storage.service.StorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileNotFoundException;

import static com.rbkmoney.file.storage.util.CheckerUtil.checkString;

@RestController
@RequiredArgsConstructor
@Slf4j
public class UploadFileController {

    private final StorageService storageService;

    @PostMapping("/file_storage/upload")
    public ResponseEntity handleFileUpload(@RequestParam(value = "file_id") String fileId,
                                           @RequestParam(value = "file") MultipartFile file) {
        try {
            log.info("Request handleFileUpload fileId: {}", fileId);
            checkString(fileId, "Bad request parameter, fileId required and not empty arg");
            storageService.uploadFile(fileId, file.getInputStream());
            ResponseEntity<Object> responseEntity = ResponseEntity.ok().build();
            log.info("Response: ResponseEntity: {}", responseEntity);
            return responseEntity;
        } catch (FileNotFoundException e) {
            log.error("Error when handleFileUpload e: ", e);
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            log.error("Error when handleFileUpload e: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
