package com.camendoza94.zoo;

import com.camendoza94.exceptions.NoMatchesFoundException;
import com.camendoza94.exceptions.ServiceNotFoundInOntologyException;
import com.camendoza94.exceptions.ServicesNotAvailableException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
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
    private final RestTemplate template = new RestTemplate();
    private static String SEMANTIC_INTERFACE_HOST;
    private static String SEMANTIC_INTERFACE_PATH;
    private static String SEMANTIC_INTERFACE_PORT;
    public static final String BASE_PATH = "semanticInterface";
    private ZooKeeperClientManager zooKeeperClientManager = new ZooKeeperClientManager();

    @RequestMapping(method = RequestMethod.POST)
    ResponseEntity<String> requestService(@RequestBody DeviceObservation observation) throws ServiceNotFoundInOntologyException, NoMatchesFoundException, ServicesNotAvailableException {
        zooKeeperClientManager = new ZooKeeperClientManager();
        String reference = observation.getDeviceReference();
        JsonParser parser = new JsonParser();
        JsonObject body = parser.parse(observation.getPayload()).getAsJsonObject();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);
        try {
            List<String> children = zooKeeperClientManager.getZNodeTree(BASE_PATH + "/" + reference);
            if (children.size() == 1 && obtainURL(children.get(0)).equals(NO_SERVICE)) {
                triggerMatching(reference);
                children = zooKeeperClientManager.getZNodeTree(BASE_PATH + "/" + reference);
            }
            if (children.size() > 1 || !obtainURL(children.get(0)).equals(NO_SERVICE)) {
                for (String child : children) {
                    if (zooKeeperClientManager.getZNodeData(child, false) == 1) {
                        String URL = obtainURL(child);
                        try {
                            zooKeeperClientManager.closeConnection();
                            return template.postForEntity("http://" + URL, entity, String.class);
                        } catch (HttpClientErrorException e) {
                            //Service returns a Bad Request response, so we assume the service is not compatible and delete the child
                            if (e.getStatusCode().equals(HttpStatus.BAD_REQUEST))
                                zooKeeperClientManager.delete(child);
                            else
                                zooKeeperClientManager.update(child, new byte[]{(byte) 0});
                        } catch (Exception e) {
                            //Service is down or could not complete request correctly
                            zooKeeperClientManager.update(child, new byte[]{(byte) 0});
                        }
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            zooKeeperClientManager.closeConnection();
        } catch (ServiceNotFoundInOntologyException | NoMatchesFoundException e) {
            zooKeeperClientManager.closeConnection();
            throw e;
        }
        zooKeeperClientManager.closeConnection();
        throw new ServicesNotAvailableException();
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
    private void triggerMatching(String deviceReference) throws NoMatchesFoundException, ServiceNotFoundInOntologyException {
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
            ResponseEntity<String> response = template.postForEntity("http://" + SEMANTIC_INTERFACE_HOST + ":" + SEMANTIC_INTERFACE_PORT + SEMANTIC_INTERFACE_PATH, deviceReference, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                String paths = response.getBody();
                try {
                    String[] services = paths.split("\n");
                    for (String path : services) {
                        if (!path.isEmpty()) {
                            List<Op> ops = new ArrayList<>();
                            if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH) == null)
                                ops.add(Op.create("/" + BASE_PATH, new byte[]{(byte) 1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                            if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH + "/" + deviceReference) == null)
                                ops.add(Op.create("/" + BASE_PATH + "/" + deviceReference, new byte[]{(byte) 1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                            String[] parts = getParts(path);
                            for (String part : parts) {
                                if (zooKeeperClientManager.getZNodeStats("/" + BASE_PATH + "/" + deviceReference + "/" + part) == null)
                                    ops.add(Op.create("/" + BASE_PATH + "/" + deviceReference + "/" + part, new byte[]{(byte) 1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
                            }
                            zooKeeperClientManager.multi(ops);
                        }
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            if (e.getStatusCode().equals(HttpStatus.PRECONDITION_FAILED))
                throw new NoMatchesFoundException();
            else if (e.getStatusCode().equals(HttpStatus.SERVICE_UNAVAILABLE))
                throw new ServiceNotFoundInOntologyException();
            throw new WebApplicationException();
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

    @ExceptionHandler
    void handleDeviceNotFoundException(NoMatchesFoundException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.PRECONDITION_FAILED.value());
    }

    @ExceptionHandler
    void handleServiceNotFoundException(ServiceNotFoundInOntologyException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value());
    }

    @ExceptionHandler
    void handleServicesNotAvailableException(ServicesNotAvailableException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value());
    }

}