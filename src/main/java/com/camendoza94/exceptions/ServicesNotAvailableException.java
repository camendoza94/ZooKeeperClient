package com.camendoza94.exceptions;

public class ServicesNotAvailableException extends Exception {
    public ServicesNotAvailableException() {
        super("All services matched are unavailable.");
    }
}
