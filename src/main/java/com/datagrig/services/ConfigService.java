package com.datagrig.services;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.datagrig.ssh.SSHConfig;
import com.datagrig.ssh.SSHKeepAlive;
import com.jcraft.jsch.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.pojo.ConnectionUrl;
import com.datagrig.pojo.ForeignKeyMetaData;
import com.datagrig.pojo.TestConnectionResult;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ConfigService {

    @Autowired
    private SSHKeepAlive sshKeepAlive;

    @Autowired
    private AppConfig appConfig;
    
    public List<File> getConnectionFolders() {
        return Arrays.stream(appConfig.getFolder().listFiles()).collect(Collectors.toList());
    }

    public List<ConnectionConfig> getConnections() {
        return getConnectionFolders().stream().map(File::getName).map(name -> {
            try {
                return this.getConnection(name);
            } catch (IOException e) {
                log.error("Error mapping connection", e);
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    public ConnectionConfig getConnection(String connectionName) throws IOException {
        ObjectMapper mapper = createObjectMapper();
        File connectionConfigFile = getConnectionConfigFile(connectionName);
        ConnectionConfig connectionConfig = mapper.readValue(connectionConfigFile, ConnectionConfig.class);
        connectionConfig.setName(connectionName);
        return connectionConfig;
    }

    public Optional<ForeignKeyMetaData[]> getCustomForeignKeys(String connectionName) throws IOException {
        ObjectMapper mapper = createObjectMapper();
        File connectionConfigFile = getCustomForeignKeysFile(connectionName);
        if(connectionConfigFile.exists()) {
            ForeignKeyMetaData[] foreignKeyMetaData = mapper.readValue(connectionConfigFile, ForeignKeyMetaData[].class);
            return Optional.of(foreignKeyMetaData);
        } else {
            return Optional.empty();
        }
    }

    protected ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    public File getConnectionFolder(String connectionName) {
        return new File(appConfig.getFolder(), connectionName);
    }

    public File getConnectionConfigFile(String connectionName) {
        File connectionFolder = getConnectionFolder(connectionName);
        return new File(connectionFolder, ConnectionConfig.FILE_NAME);
    }

    public File getCustomForeignKeysFile(String connectionName) {
        File connectionFolder = getConnectionFolder(connectionName);
        return new File(connectionFolder, ForeignKeyMetaData.FILE_NAME);
    }

	public ConnectionConfig saveConnection(String connectionName, ConnectionConfig connectionConfig) throws JsonGenerationException, JsonMappingException, IOException {
        File connectionFile = getConnectionConfigFile(connectionName);
        if(!connectionFile.getParentFile().exists()) {
        	connectionFile.getParentFile().mkdirs();
        }
        ObjectMapper objectMapper = createObjectMapper();
        connectionConfig.setName(connectionName);
        objectMapper.writeValue(connectionFile, connectionConfig);
		return getConnection(connectionName);
	}

	public void deleteConnection(String connectionName) {
        File connectionFile = getConnectionFolder(connectionName);
        FileSystemUtils.deleteRecursively(connectionFile);
	}

	public TestConnectionResult testConnection(String connectionName, ConnectionConfig connectionConfig) throws ClassNotFoundException, SQLException, JSchException {
        String jdbcUrl;
        if(connectionConfig.isSsh()) {
            ConnectionUrl connectionUrl = new ConnectionUrl(connectionConfig.getUrl());
        	// Pair<Session, Integer> pairSession = createSshConnectionAndConnect(connectionConfig);
        	connectionUrl.setHost("localhost");
            int port = sshKeepAlive.getForwardedPort(connectionConfig);
        	connectionUrl.setPort(port);
        	jdbcUrl = connectionUrl.toUrl();
        } else {
        	jdbcUrl = connectionConfig.getUrl();
        }
		Class.forName(connectionConfig.getDriver());
		try(Connection con = DriverManager.getConnection(jdbcUrl, connectionConfig.getUser(), connectionConfig.getPassword())) {
			try(Statement st = con.createStatement()) {
				st.executeQuery("select 1");
				if(ObjectUtils.firstNonNull(connectionConfig.getAliasQuery(), "").trim().length() > 0) {
					ResultSet rs = st.executeQuery(connectionConfig.getAliasQuery());
					rs.next();
					String alias = rs.getString(1);
					return TestConnectionResult.builder()
							.alias(alias)
							.build();
				}
			}
		}
		return TestConnectionResult.builder()
				.build();
	}

	/*
    public Pair<Session, Integer> createSshConnectionAndConnect(ConnectionConfig configConnection) throws JSchException {
    	JSch jsch=new JSch();

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
		

        session.connect();

        ConnectionUrl connectionUrl = new ConnectionUrl(configConnection.getUrl());
        int assingedPort = session.setPortForwardingL(0, connectionUrl.getHost(), connectionUrl.getPort());


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

        return Pair.of(session, assingedPort);
	}
	*/

}
