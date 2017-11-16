package com.camendoza94.zoo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.zookeeper.KeeperException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.management.ServiceNotFoundException;

@RestController
@RequestMapping("/zoo")
class ZooKeeperController {

    private final RestTemplate template = new RestTemplate();
    private final ZooKeeperClientManager zooKeeperClientManager = new ZooKeeperClientManager();
    private static final String SEMANTIC_INTERFACE_HOST = "http://localhost:1234";
    private static final String SEMANTIC_INTERFACE_PATH = "/interface";

    @RequestMapping(method = RequestMethod.POST)
    ResponseEntity<String> requestService(@RequestBody DeviceObservation observation) {
        String URL = null;
        try {
            URL = triggerMatching(observation.getDeviceId());
        } catch (ServiceNotFoundException e) {
            //TODO No Matching
        }
        JsonParser parser = new JsonParser();
        JsonObject body = parser.parse(observation.getPayload()).getAsJsonObject();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);
        //TODO Catch 5xx
        return template.postForEntity(URL, entity, String.class);
    }

    /*
        Trigger semantic matching when ZooKeeper does not find a corresponding znode
     */
    private String triggerMatching(String deviceId) throws ServiceNotFoundException {
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
                return "http://" + services[0];
            } catch (KeeperException e) {
                //TODO catch according to Zookeeper exception code
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
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