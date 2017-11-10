package com.camendoza94.zoo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/zoo")
class ZooKeeperController {

    private final RestTemplate template = new RestTemplate();
    private final ZooKeeperClientManager zooKeeperClientManager = new ZooKeeperClientManager();
    private static final String SEMANTIC_INTERFACE_HOST = "http://localhost:1234";
    private static final String SEMANTIC_INTERFACE_PATH = "/interface";

    @RequestMapping(method = RequestMethod.POST)
    ResponseEntity<String> requestService(@RequestBody DeviceObservation observation) {
        String URL = triggerMatching(observation.getDeviceId());
        JsonParser parser = new JsonParser();
        JsonObject body = parser.parse(observation.getPayload()).getAsJsonObject();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);
        //TODO get labels and types and use them on the POST
        return template.postForEntity(URL, entity, String.class);
    }

    /*
        Trigger semantic matching when ZooKeeper does not find a corresponding znode
     */
    private String triggerMatching(String deviceId) {
        ResponseEntity<String> response = template.postForEntity(SEMANTIC_INTERFACE_HOST + SEMANTIC_INTERFACE_PATH, deviceId, String.class);
        if (response.getStatusCode().is2xxSuccessful()) {
            return response.getBody();
        }
        return null;
    }

}