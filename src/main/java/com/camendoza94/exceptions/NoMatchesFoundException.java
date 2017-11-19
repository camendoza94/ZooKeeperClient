package com.camendoza94.exceptions;

public class NoMatchesFoundException extends Exception {
    public NoMatchesFoundException() {
        super("No matches found for given device.");
    }
}
