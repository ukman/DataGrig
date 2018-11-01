package com.datagrig.controllers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.rest.webmvc.ResourceNotFoundException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.datagrig.pojo.ConnectionCatalog;
import com.datagrig.services.ConnectionService;
import com.jcraft.jsch.JSchException;

@RestController
@RequestMapping("/aliases")
public class AliasController {

    @Autowired
    private ConnectionService connectionService;

    @RequestMapping("{alias}")
    public ConnectionCatalog lookupAlias(@PathVariable("alias") String alias) throws IOException, SQLException, JSchException {
        Optional<ConnectionCatalog> connectionCatalog = connectionService.lookupAlias(alias);
        return connectionCatalog.orElseThrow(ResourceNotFoundException::new);
    }
}
