package com.camendoza94.zoo;

class DeviceObservation {

    private String payload;

    private String deviceReference;

    String getDeviceReference() {
        return deviceReference;
    }

    String getPayload() {
        return payload;
    }

    public void setDeviceReference(String deviceReference) {
        this.deviceReference = deviceReference;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
