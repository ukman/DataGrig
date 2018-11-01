package com.datagrig;

import java.util.List;

import com.datagrig.pojo.ConnectionUrl;
import com.datagrig.ssh.SSHConfig;
import lombok.*;

@Data
@Builder
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class ConnectionConfig implements SSHConfig {
    public static final String FILE_NAME = "connection-config.json";

    private String name;
    private String driver;
    private String url;
    private String user;
    private String password;
    private boolean ssh;
    private String sshHost;
    private int sshPort;
    private String sshUser;
    private String sshPassword;
    
    /**
     * List of catalogs to be excluded.
     */
    private List<String> excludeCatalogs;
    

    /**
     * Alias query should return true/false for alias name as the only param.
     * E.g.
     * select exists(select * from options where db_name = ?)
     */
    private String aliasQuery;

    @Override
    public String getHostForward() {
        ConnectionUrl connectionUrl = new ConnectionUrl(this.url);
        return connectionUrl.getHost();
    }

    @Override
    public int getPortForward() {
        ConnectionUrl connectionUrl = new ConnectionUrl(this.url);
        return connectionUrl.getPort();
    }

}
