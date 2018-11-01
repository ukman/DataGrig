package com.datagrig.ssh;

/**
 * Created by ukman on 10/12/18.
 */
public interface SSHConfig {
    String getSshHost();
    int getSshPort();
    String getSshUser();
    String getSshPassword();
    String getHostForward();
    int getPortForward();
}
