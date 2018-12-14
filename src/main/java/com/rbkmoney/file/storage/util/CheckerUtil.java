package com.rbkmoney.file.storage.util;

import com.google.common.base.Strings;

public class CheckerUtil {

    public static void checkString(String string, String exMessage) throws IllegalArgumentException {
        if (Strings.isNullOrEmpty(string)) {
            throw new IllegalArgumentException(exMessage);
        }
    }
}
