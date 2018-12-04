package com.rbkmoney.file.storage.util;

import com.google.common.base.Strings;
import org.springframework.web.multipart.MultipartFile;

import java.util.regex.Pattern;

public class CheckerUtil {

    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9 ,-_.]*$");

    public static void checkString(String string, String exMessage) throws RuntimeException {
        if (Strings.isNullOrEmpty(string)) {
            throw new RuntimeException(exMessage);
        }
    }

    public static void checkFileName(String string, String exMessage) throws RuntimeException {
        if (!FILE_NAME_PATTERN.matcher(string).matches()) {
            throw new RuntimeException(exMessage);
        }
    }


    public static void checkFile(MultipartFile file, String exMessage) throws RuntimeException {
        if (file.isEmpty()) {
            throw new RuntimeException(exMessage);
        }
    }
}
