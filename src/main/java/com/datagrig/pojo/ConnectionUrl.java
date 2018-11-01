package com.datagrig.pojo;

import lombok.*;
import org.apache.commons.lang3.ObjectUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ConnectionUrl {
    private static final String CONNECTION_RE = "([^:]+):([^:]*)://([^:/]+)(:[0-9]+)?/([^?]*)(\\?.+)?"; //jdbc:postgresql://localhost:5432/cyfoman
    private static final Pattern CONNECTION_PATTERN = Pattern.compile(CONNECTION_RE);

    private String protocol;
    private String type;
    private String host;
    private int port;
    private String catalog;
    private String params;

    public String toUrl() {
        return protocol + ":" + type + "://" + host + (port > 0 ? ":" + port : "") + "/" + catalog + ObjectUtils.firstNonNull(params, "");
    }

    public ConnectionUrl(String url) {
        Matcher m = CONNECTION_PATTERN.matcher(url);
        if(m.matches()) {
            this.protocol = m.group(1);
            this.type = m.group(2);
            this.host = m.group(3);
            String sPort = m.group(4);
            this.port = sPort == null || sPort.length() == 0 ? 0 : Integer.parseInt(sPort.substring(1));
            this.catalog = m.group(5);
            this.params = m.group(6);
        } else {
            throw new IllegalArgumentException(String.format("Cannot parse url '?'", url));
        }
    }
}
