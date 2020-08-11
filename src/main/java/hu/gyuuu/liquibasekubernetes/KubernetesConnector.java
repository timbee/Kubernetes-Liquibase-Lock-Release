package hu.gyuuu.liquibasekubernetes;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

public class KubernetesConnector {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesConnector.class);

    public static final String POD_PHASE_PENDING = "Pending";
    public static final String POD_PHASE_RUNNING = "Running";
    public static final int HTTP_STATUS_NOT_FOUND = 404;
    private final boolean connected;
    private final String podName;
    private final String podNamespace;

    public static final class InstanceHolder {
        private static final KubernetesConnector INSTANCE = new KubernetesConnector();
    }

    public static KubernetesConnector getInstance() {
        return InstanceHolder.INSTANCE;
    }

    private KubernetesConnector() {
        podName = System.getenv().get("POD_NAME");
        podNamespace = System.getenv().get("POD_NAMESPACE");
        if (StringUtils.isNotBlank(podName) && StringUtils.isNotBlank(podNamespace)) {
            connected = connect();
        } else {
            connected = false;
            LOG.debug("POD_NAME or POD_NAMESPACE is not configured, Liquibase - Kubernetes integration disabled");
        }
    }

    private boolean connect() {
        try {
            LOG.trace("Create client with from cluster configuration");
            ApiClient client = Config.fromCluster();

            Configuration.setDefaultApiClient(client);

            LOG.trace("BasePath: " + client.getBasePath());
            LOG.trace("Authentication: " + client.getAuthentications().entrySet().stream().map(entry -> entry.getKey() + ":" + entry.getValue()).collect(Collectors.joining(", ")));

            CoreV1Api api = new CoreV1Api();
            LOG.trace("Reading pod status, Pod name: " + podName + " Pod namespace: " + podNamespace);
            V1Pod pod = api.readNamespacedPodStatus(podName, podNamespace, "true");
            String podPhase = pod.getStatus().getPhase();
            LOG.trace("Pod phase:" + podPhase);
            LOG.trace("Connected to Kubernetes using fromCluster configuration");
            return true;
        } catch (IOException | ApiException e) {
            LOG.debug("Connection fail to Kubernetes cluster using fromCluster configuration");
            LOG.trace("Pod status read error", e);
            return false;
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public String getPodName() {
        return podName;
    }

    public String getPodNamespace() {
        return podNamespace;
    }

    public boolean isCurrentPod(String podNamespace, String podName) {
        if (podNamespace == null || podName == null) {
            return false;
        }
        return podNamespace.equals(this.podNamespace) && podName.equals(this.podName);
    }

    public Boolean isPodActive(String podNamespace, String podName) {
        try {
            CoreV1Api api = new CoreV1Api();
            LOG.trace("Reading pod status, Pod name: {} Pod namespace: {}", podName, podNamespace);
            V1Pod pod = null;

            pod = api.readNamespacedPodStatus(podName, podNamespace, "true");
            String podPhase = pod.getStatus().getPhase();

            if (POD_PHASE_PENDING.equals(podPhase) || POD_PHASE_RUNNING.equals(podPhase)) {
                LOG.trace("Pod is active, phase: {} ", podPhase);
                return true;
            } else {
                LOG.trace("Pod is inactive, phase: {}", podPhase);
                return false;
            }
        } catch (ApiException e) {
            if (e.getCode() == HTTP_STATUS_NOT_FOUND) {
                LOG.trace("Can't find pod");
                return false;
            }
            LOG.info("Can't read Pod status:" + podNamespace + ":" + podName);
            LOG.info("Pod status read error", e);
            return null;
        }
    }
}
