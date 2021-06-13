package com.test.model;

public class EventDTO {
	private String id;
	private String type;
	private String host;
	private Long duration;

	public EventDTO(String id, String type, String host, Long duration) {
		this.id = id;
		this.type = type;
		this.host = host;
		this.duration = duration;
	}

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	public String getHost() {
		return host;
	}

	public Long getDuration() {
		return duration;
	}
}
