package com.datagrig;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import lombok.experimental.FieldNameConstants;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.datagrig.cache.CacheConfig;

import lombok.Data;

@ConfigurationProperties("datagrig")
@Data
@Component
@EnableWebMvc
@FieldNameConstants
public class AppConfig {
    private File folder;
    private String rebootPassword;
    private String[] labelColumnNames;
}
