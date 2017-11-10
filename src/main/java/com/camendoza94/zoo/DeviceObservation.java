package com.camendoza94.zoo;

class DeviceObservation {

    private String payload;

    private String deviceId;

    String getDeviceId() {
        return deviceId;
    }

    String getPayload() {
        return payload;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
