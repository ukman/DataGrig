package com.datagrig;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.File;

@ConfigurationProperties("datagrig")
@Data
@Component
public class AppConfig {
    private File folder;
}
