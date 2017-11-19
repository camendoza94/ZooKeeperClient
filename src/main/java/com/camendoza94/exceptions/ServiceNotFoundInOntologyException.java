package com.camendoza94.exceptions;

public class ServiceNotFoundInOntologyException extends Exception {
    public ServiceNotFoundInOntologyException() {
        super("Service matched was not found in ontology.");
    }
}
