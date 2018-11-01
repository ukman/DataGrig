package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BinaryInfo {
	private String contentType;
	private long size;
}
