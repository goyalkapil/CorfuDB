package org.corfudb.runtime.exceptions;

/**
 * This Exception is thrown when there is a modification to a file in between
 * a bulk copy.
 * <p>
 * Created by zlokhandwala on 2/16/17.
 */
public class FileModificationException extends Exception {

    public FileModificationException(String message) {
        super(message);
    }
}
