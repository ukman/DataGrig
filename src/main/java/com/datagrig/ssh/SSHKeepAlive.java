package com.datagrig.ssh;

import com.datagrig.AppConfig;
import com.datagrig.services.ConfigService;
import com.jcraft.jsch.*;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Service
@Slf4j
public class SSHKeepAlive {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ConfigService configService;

    @Data
    @Builder
    @ToString
    @EqualsAndHashCode(of = "sshConfig")
    private static class KeepAliveItem {
        private SSHConfig sshConfig;
        private Session session;
        private int localForwardPort;
        private int reconnectCount;
        public void incrementReconnectCount() {
            reconnectCount++;
        }
    }

    private List<KeepAliveItem> sessions = new ArrayList<>();

    /*
    public void postConstruct22() {
        configService.getConnections().forEach(config -> {
            if(config.isSsh()) {
                try {
                    addConfig(config);
                } catch (JSchException e) {
                    log.error("Error starting keep alive process", e);
                    throw new RuntimeException(e);
                }
            }
        });
    }
    */

    public void addConfig(SSHConfig sshConfig) throws JSchException {
        if(!containsConfig(sshConfig)) {
            KeepAliveItem item = createSshConnectionAndConnect(sshConfig, 0);
            sessions.add(item);
        }
    }

    private boolean containsConfig(SSHConfig sshConfig) {
        return sessions.stream().anyMatch(item -> item.getSshConfig().equals(sshConfig));
    }

    public void remove(SSHConfig sshConfig) throws JSchException {
        for (Iterator<KeepAliveItem> it = sessions.iterator(); it.hasNext(); ) {
            KeepAliveItem item = it.next();
            if(item.getSshConfig() == sshConfig) {
                if(item.getSession().isConnected()) {
                    item.getSession().delPortForwardingL(item.getLocalForwardPort());
                    item.getSession().disconnect();
                    it.remove();
                }
            }
        }
    }

    @Scheduled(fixedDelay=10000)
    protected void keepAlive() {
        for(KeepAliveItem p : sessions) {
            getSpringProxy().keepAlive(p);
        }
    }

    @Async
    protected void keepAlive(KeepAliveItem item) {
        Session session = item.getSession();
        boolean needReconnect = true;
        if(session.isConnected()) {
            ChannelExec channel = null;
            try {
                channel = (ChannelExec) session.openChannel("exec");
                channel.setCommand("echo 1");
                channel.connect(2000);
                while (!channel.isClosed()) {
                    Thread.sleep(100);
                }
                int exitStatus = channel.getExitStatus();
                needReconnect = exitStatus != 0;

            } catch (Exception e) {
                log.error("Error trying to keep alive for " + item, e);
            } finally {
                if(channel != null) {
                    channel.disconnect();
                }
            }
        }
        if (needReconnect) {
            try {
                session.delPortForwardingL(item.getLocalForwardPort());
                session.disconnect();
            } catch (JSchException e) {
                log.error("Error reconnecting", e);
            }
            try {
                KeepAliveItem newItem = createSshConnectionAndConnect(item.getSshConfig(), item.getLocalForwardPort());
                item.setSession(newItem.getSession());
                item.incrementReconnectCount();
                log.info("Reconnected to " + item.getSshConfig() + " " + item.getReconnectCount() + " times.");
            } catch (JSchException e) {
                log.error("Error reconnecting", e);
            }
        }
    }


    private JSch jsch=new JSch();

    private Session createSshConnection(SSHConfig configConnection) throws JSchException {
        Session session = jsch.getSession(configConnection.getSshUser(), configConnection.getSshHost(), configConnection.getSshPort());

        UserInfo ui = new UserInfo() {

            @Override
            public void showMessage(String message) {
                // TODO Auto-generated method stub
                log.info(message);
            }

            @Override
            public boolean promptYesNo(String message) {
                log.info("promptYesNo : " + message);
                return true;
            }

            @Override
            public boolean promptPassword(String message) {
                log.info("promptPassword : " + message);
                return true;
            }

            @Override
            public boolean promptPassphrase(String message) {
                log.info("promptPassphrase : " + message);
                return false;
            }

            @Override
            public String getPassword() {
                return configConnection.getSshPassword();
            }

            @Override
            public String getPassphrase() {
                return null;
            }
        };
        session.setUserInfo(ui);
        return session;
    }

    public KeepAliveItem createSshConnectionAndConnect(SSHConfig configConnection, int port) throws JSchException {

        Session session = createSshConnection(configConnection);

        session.connect();

        int assingedPort = session.setPortForwardingL(port, configConnection.getHostForward(), configConnection.getPortForward());
        log.info("Connected to " + configConnection.getSshHost() + " connected = " + session.isConnected());

        return KeepAliveItem.builder()
                .sshConfig(configConnection)
                .session(session)
                .localForwardPort(assingedPort)
                .build();
        /*
        Channel channel=session.openChannel("exec");
        ((ChannelExec)channel).setCommand("ls232");

        // X Forwarding
        // channel.setXForwarding(true);

        //channel.setInputStream(System.in);
        channel.setInputStream(null);

        //channel.setOutputStream(System.out);

        //FileOutputStream fos=new FileOutputStream("/tmp/stderr");
        //((ChannelExec)channel).setErrStream(fos);
        ((ChannelExec)channel).setErrStream(System.err);

        try {
            InputStream in = channel.getInputStream();

            channel.connect();
            byte[] tmp=new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    System.out.print(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if (in.available() > 0) continue;
                    System.out.println("exit-status: " + channel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        channel.disconnect();

        return org.apache.commons.lang3.tuple.Pair.of(session, assingedPort);
        */

    }

    private SSHKeepAlive getSpringProxy() {
        return applicationContext.getBean(SSHKeepAlive.class);
    }

    public int getForwardedPort(SSHConfig sshConfig) {
        KeepAliveItem keepAliveItem = sessions.stream().filter(item -> item.getSshConfig().equals(sshConfig)).findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot find port for " + sshConfig));
        return keepAliveItem.getLocalForwardPort();
    }

}
