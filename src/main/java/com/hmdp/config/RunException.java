package com.hmdp.config;

import org.springframework.context.annotation.Configuration;


public class RunException extends RuntimeException{
    public RunException(String message) {
        super(message);
    }
}
