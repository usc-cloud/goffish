package edu.usc.pgroup.gml.generator;

public class VehicleInfo {

	private String licenseState;
	
	private String licensePlate;
	
	private String vehicleType;
	
	private String vehicleColor;
	
	public VehicleInfo(String licenseState, String licensePlate, String vehicleType, String vehicleColor){
		this.licenseState = licenseState;
		this.licensePlate = licensePlate;
		this.vehicleType = vehicleType;
		this.vehicleColor = vehicleColor;
	}

	public String getLicenseState() {
		return licenseState;
	}

	public String getLicensePlate() {
		return licensePlate;
	}

	public String getVehicleType() {
		return vehicleType;
	}

	public String getVehicleColor() {
		return vehicleColor;
	}
}
