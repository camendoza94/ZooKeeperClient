package com.camendoza94.zoo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.zookeeper.KeeperException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import javax.management.ServiceNotFoundException;
import java.util.List;

@RestController
@RequestMapping("/zoo")
class ZooKeeperController {

    private static final String NO_SERVICE = "No service";
    private static final String SERVICE_NOT_FOUND = "Service not found";
    private final RestTemplate template = new RestTemplate();
    private static final String SEMANTIC_INTERFACE_HOST = "http://localhost:1234";
    private static final String SEMANTIC_INTERFACE_PATH = "/interface";
    private static final String BASE_PATH = "semanticInterface";
    private ZooKeeperClientManager zooKeeperClientManager = new ZooKeeperClientManager();

    @RequestMapping(method = RequestMethod.POST)
    ResponseEntity<String> requestService(@RequestBody DeviceObservation observation) {
        zooKeeperClientManager = new ZooKeeperClientManager();
        String id = observation.getDeviceId();
        JsonParser parser = new JsonParser();
        JsonObject body = parser.parse(observation.getPayload()).getAsJsonObject();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);
        try {
            List<String> children = zooKeeperClientManager.getZNodeTree(BASE_PATH + "/" + id);
            if (children.size() == 1 && obtainURL(children.get(0)).equals(NO_SERVICE)) {
                triggerMatching(id);
                children = zooKeeperClientManager.getZNodeTree(BASE_PATH + "/" + id);
            }
            if (children.size() > 1 || !obtainURL(children.get(0)).equals(NO_SERVICE)) {
                for (String child : children) {
                    if (zooKeeperClientManager.getZNodeData(child, false) == 1) {
                        String URL = obtainURL(child);
                        ResponseEntity<String> request = template.postForEntity("http://" + URL, entity, String.class);
                        if (request.getStatusCode().is2xxSuccessful()) {
                            zooKeeperClientManager.closeConnection();
                            return request;
                        } else {
                            zooKeeperClientManager.update(child, new byte[]{(byte) 0});
                        }
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        } catch (ServiceNotFoundException e) {
            return ResponseEntity.badRequest().body("Could not find a matching service.");
        } finally {
            zooKeeperClientManager.closeConnection();
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Services are not available.");
    }

    private String obtainURL(String path) {
        int start = path.indexOf("/", BASE_PATH.length() + 2);
        if (start != -1)
            return path.substring(start + 1);
        else
            return NO_SERVICE;
    }

    /*
        Trigger semantic matching when ZooKeeper does not find a corresponding znode
     */
    private void triggerMatching(String deviceId) throws ServiceNotFoundException {
        try {
            ResponseEntity<String> response = template.postForEntity(SEMANTIC_INTERFACE_HOST + SEMANTIC_INTERFACE_PATH, deviceId, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                String paths = response.getBody();
                if (paths.equals(SERVICE_NOT_FOUND))
                    throw new ServiceNotFoundException();
                try {
                    //TODO use multi() to create paths
                    String[] services = paths.split("\n");
                    for (String path : services) {
                        if (!path.isEmpty()) {
                            if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH) == null)
                                zooKeeperClientManager.create("/" + BASE_PATH, new byte[]{(byte) 1});
                            if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH + "/" + deviceId) == null)
                                zooKeeperClientManager.create("/" + BASE_PATH + "/" + deviceId, new byte[]{(byte) 1});
                            String[] parts = getParts(path);
                            for (String part : parts) {
                                if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH + "/" + deviceId + "/" + part) == null)
                                    zooKeeperClientManager.create("/" + BASE_PATH + "/" + deviceId + "/" + part, new byte[]{(byte) 1});
                            }
                        }
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (HttpClientErrorException e) {
            throw new ServiceNotFoundException();
        } catch (HttpServerErrorException e) {
            throw new ServiceNotFoundException(); //TODO Change to another exception
        }

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