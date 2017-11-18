package com.camendoza94.zoo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import javax.management.ServiceNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@RestController
@RequestMapping("/zoo")
public class ZooKeeperController {

    private static final String NO_SERVICE = "No service";
    private static final String SERVICE_NOT_FOUND = "Service not found";
    private final RestTemplate template = new RestTemplate();
    private static String SEMANTIC_INTERFACE_HOST;
    private static String SEMANTIC_INTERFACE_PATH;
    private static String SEMANTIC_INTERFACE_PORT;
    public static final String BASE_PATH = "semanticInterface";
    private final ZooKeeperClientManager zooKeeperClientManager = new ZooKeeperClientManager();

    @RequestMapping(method = RequestMethod.POST)
    ResponseEntity<String> requestService(@RequestBody DeviceObservation observation) {
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
                        try {
                            return template.postForEntity("http://" + URL, entity, String.class);
                        } catch (Exception e) {
                            //Server is down or could not complete request correctly
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

    private static String obtainURL(String path) {
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
            if (SEMANTIC_INTERFACE_HOST == null || SEMANTIC_INTERFACE_PATH == null || SEMANTIC_INTERFACE_PORT == null) {

                Properties prop = new Properties();
                InputStream input;
                try {

                    input = new FileInputStream("./src/main/resources/semantic.properties");
                    prop.load(input);
                    SEMANTIC_INTERFACE_HOST = prop.getProperty("server.host");
                    SEMANTIC_INTERFACE_PORT = prop.getProperty("server.port");
                    SEMANTIC_INTERFACE_PATH = prop.getProperty("server.path");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ResponseEntity<String> response = template.postForEntity("http://" + SEMANTIC_INTERFACE_HOST + ":" + SEMANTIC_INTERFACE_PORT + SEMANTIC_INTERFACE_PATH, deviceId, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                String paths = response.getBody();
                if (paths.equals(SERVICE_NOT_FOUND))
                    throw new ServiceNotFoundException();
                try {
                    String[] services = paths.split("\n");
                    for (String path : services) {
                        if (!path.isEmpty()) {
                            List<Op> ops = new ArrayList<>();
                            if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH) == null)
                                ops.add(Op.create("/" + BASE_PATH, new byte[]{(byte) 1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                            if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH + "/" + deviceId) == null)
                                ops.add(Op.create("/" + BASE_PATH + "/" + deviceId, new byte[]{(byte) 1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                            String[] parts = getParts(path);
                            for (String part : parts) {
                                if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH + "/" + deviceId + "/" + part) == null)
                                    ops.add(Op.create("/" + BASE_PATH + "/" + deviceId + "/" + part, new byte[]{(byte) 1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                            }
                            zooKeeperClientManager.multi(ops);
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