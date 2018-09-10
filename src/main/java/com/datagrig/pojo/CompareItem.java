package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CompareItem {
	public static enum Severity {
		INFO,
		WARN,
		ERROR
	}
	private String message;
	private Severity severity;
	
}
