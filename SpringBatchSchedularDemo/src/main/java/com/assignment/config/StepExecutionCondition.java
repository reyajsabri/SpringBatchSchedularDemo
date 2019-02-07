package com.assignment.config;

public enum StepExecutionCondition {
	
	STOP("Stop"), PROCEED("Proceed");
	private String name;
	
	public String getName() {
		return name;
	}

	StepExecutionCondition(String name){
		this.name = name;
	}
}
