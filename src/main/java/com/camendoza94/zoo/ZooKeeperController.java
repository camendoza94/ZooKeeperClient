package com.camendoza94.zoo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.zookeeper.KeeperException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.management.ServiceNotFoundException;
import java.util.List;

@RestController
@RequestMapping("/zoo")
class ZooKeeperController {

    private final RestTemplate template = new RestTemplate();
    private final ZooKeeperClientManager zooKeeperClientManager = new ZooKeeperClientManager();
    private static final String SEMANTIC_INTERFACE_HOST = "http://localhost:1234";
    private static final String SEMANTIC_INTERFACE_PATH = "/interface";

    @RequestMapping(method = RequestMethod.POST)
    ResponseEntity<String> requestService(@RequestBody DeviceObservation observation) {
        String id = observation.getDeviceId();
        JsonParser parser = new JsonParser();
        JsonObject body = parser.parse(observation.getPayload()).getAsJsonObject();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);
        try {
            List<String> children = zooKeeperClientManager.getZNodeChildren("/" + id);
            if (children.isEmpty()) {
                triggerMatching(id);
                children = zooKeeperClientManager.getZNodeChildren("/" + id);
            }
            if (children != null) {
                for (String child : children) {
                    //TODO get path recursively?
                    if (zooKeeperClientManager.getZNodeData(child, false) == 1) {
                        ResponseEntity<String> request = template.postForEntity(child, entity, String.class);
                        if (request.getStatusCode().is2xxSuccessful())
                            return request;
                        else {
                            zooKeeperClientManager.update(child, new byte[]{(byte) 0});
                        }
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        } catch (ServiceNotFoundException e) {
            return ResponseEntity.badRequest().body("Could not find a matching service.");
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Services are not available.");
    }

    /*
        Trigger semantic matching when ZooKeeper does not find a corresponding znode
     */
    private void triggerMatching(String deviceId) throws ServiceNotFoundException {
        ResponseEntity<String> response = template.postForEntity(SEMANTIC_INTERFACE_HOST + SEMANTIC_INTERFACE_PATH, deviceId, String.class);
        if (response.getStatusCode().is2xxSuccessful()) {
            String paths = response.getBody();
            if (paths.isEmpty())
                throw new ServiceNotFoundException();
            try {
                //TODO use multi() to create paths
                String[] services = paths.split("\n");
                for (String path : services)
                    if (!path.isEmpty()) {
                        if (zooKeeperClientManager.getZNodeStats("/" + deviceId) == null)
                            zooKeeperClientManager.create("/" + deviceId, new byte[]{(byte) 1});
                        String[] parts = getParts(path);
                        for (String part : parts) {
                            if (zooKeeperClientManager.getZNodeStats("/" + deviceId + "/" + part) == null)
                                zooKeeperClientManager.create("/" + deviceId + "/" + part, new byte[]{(byte) 1});
                        }
                    }
                zooKeeperClientManager.closeConnection();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        } else if (response.getStatusCode().is4xxClientError())
            throw new ServiceNotFoundException();
    }


    private String[] getParts(String path) {
        String[] sections = path.split("/");
        String[] parts = new String[sections.length];
        parts[0] = sections[0];
        for (int i = 1; i < sections.length; i++) {
            parts[i] = parts[i - 1] + "/" + sections[i];
        }
        return parts;
    }

}