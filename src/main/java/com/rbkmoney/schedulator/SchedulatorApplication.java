package com.rbkmoney.schedulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication
public class SchedulatorApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulatorApplication.class, args);
    }

}
