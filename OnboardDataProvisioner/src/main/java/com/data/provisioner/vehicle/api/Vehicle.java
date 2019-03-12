package com.data.provisioner.vehicle.api;

public interface Vehicle {
	
	public void start();
	
	public void stop();
	
	public void restart(final String reason);

}
