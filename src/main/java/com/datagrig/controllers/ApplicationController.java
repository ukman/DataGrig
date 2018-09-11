package com.datagrig.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datagrig.AppConfig;

@RestController
@RequestMapping("/app")
public class ApplicationController {
	
	@Autowired
	private AppConfig appConfig;
	
	@RequestMapping("/reboot")
	public void reboot(@RequestParam("password")String pass) {
		if(pass.equals(appConfig.getRebootPassword())) {
			System.exit(0);
		}
	}
}
