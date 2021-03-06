package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SequenceMetaData {
	private String name;
	private long value;
	private long minValue;
	private long maxValue;
	private int increment;
}
