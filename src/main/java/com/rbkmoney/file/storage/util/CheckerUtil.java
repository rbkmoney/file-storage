package com.rbkmoney.file.storage.util;

import com.google.common.base.Strings;
import org.springframework.web.multipart.MultipartFile;

import java.util.regex.Pattern;

public class CheckerUtil {

    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("^[а-яА-Яa-zA-Z0-9 ,-_.]*$");

    public static void checkString(String string, String exMessage) throws IllegalArgumentException {
        if (Strings.isNullOrEmpty(string)) {
            throw new IllegalArgumentException(exMessage);
        }
    }

    public static void checkFileName(String string, String exMessage) throws IllegalArgumentException {
        if (!FILE_NAME_PATTERN.matcher(string).matches()) {
            throw new IllegalArgumentException(exMessage);
        }
    }


    public static void checkFile(MultipartFile file, String exMessage) throws IllegalArgumentException {
        if (file.isEmpty()) {
            throw new IllegalArgumentException(exMessage);
        }
    }
}
