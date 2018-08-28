package com.datagrig;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

@Slf4j
@SpringBootApplication
@PropertySources({
        @PropertySource("classpath:application.properties")
})
public class DataGrigApp {
    public static void main(String[] args) {
        SpringApplication.run(DataGrigApp.class, args);
        log.debug("Data Grig App is stared");
    }
}
