package com.poc.beam.beampoc;

import java.io.Serializable;

public class IotEvent implements Serializable {

	private String deviceId;
	private String name;
	private String description;
	private Long eventtime;
	private Double temperature;
	private String unit;
	
	public IotEvent() {
		
	}
	
    public IotEvent(String deviceId, String name, String description,Long eventtime,Double temperature,String temperature_unit) {
        this.deviceId = deviceId;
        this.name = name;
        this.description = description;
        this.eventtime = eventtime;
        this.temperature = temperature;
        this.unit = unit;
    }

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getEventtime() {
		return eventtime;
	}

	public void setEventtime(Long eventtime) {
		this.eventtime = eventtime;
	}

	public Double getTemperature() {
		return temperature;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}
    
    
    
	
}

