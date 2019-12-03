package com.rbkmoney.file.storage.util;

import com.google.common.base.Strings;

public class CheckerUtil {

    public static void checkString(String string, String exMessage) {
        if (Strings.isNullOrEmpty(string) || string.contains(" ")) {
            throw new IllegalArgumentException(exMessage);
        }
    }
}
