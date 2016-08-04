package com.datastax.loader.util;

import java.io.File;

/**
 * @author Michael Arndt
 */
public class FileUtils {
    public static boolean checkFile(String truststorePath, String msg2) {
        File tfile;
        if (null != truststorePath) {
            tfile = new File(truststorePath);
            if (!tfile.isFile()) {
                System.err.println(msg2);
                return true;
            }
        }
        return false;
    }
}
