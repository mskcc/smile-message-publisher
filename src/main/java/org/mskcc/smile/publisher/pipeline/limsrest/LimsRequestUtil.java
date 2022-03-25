package org.mskcc.smile.publisher.pipeline.limsrest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author ochoaa
 */
@Component
public class LimsRequestUtil {
    @Value("${lims.base_url}")
    private String limsBaseUrl;

    @Value("${lims.username}")
    private String limsUsername;

    @Value("${lims.password}")
    private String limsPassword;

    @Value("${lims.request_samples_endpoint}")
    private String limsRequestSamplesEndpoint;

    @Value("${lims.sample_manifest_endpoint}")
    private String limsSampleManifestEndpoint;

    @Value("${lims.request_deliveries_endpoint}")
    private String limsRequestDeliveriesEndpoint;

    protected Map<String, List<String>> limsRequestErrors = new HashMap<>();
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd");
    private ObjectMapper mapper =  new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(LimsRequestUtil.class);

    /**
     * Returns list of request ids as strings based on the start date provided.
     * If end date is also provided then the set of request ids returned will be
     * filtered.
     *
     * @param startDate
     * @param endDate
     * @return List
     * @throws Exception
     */
    public List<String> getRequestIdsByDate(String startDate, String endDate) throws Exception {
        // dates have already been validated during app startup
        Date startTimestamp = DATE_FORMAT.parse(startDate);
        Date endTimestamp = null;
        if (endDate != null) {
            endTimestamp = DATE_FORMAT.parse(endDate);
        }

        // get start date as milliseconds and fetch set of requests from LimsRest
        List<String> requestIds = getLimsRequestIdsByTimestamp(startTimestamp, endTimestamp);
        return requestIds;
    }

    /**
     * Calls LimsRest and returns request ids given the start/end timestamps.
     * @param startTimestamp
     * @param endTimestamp
     * @return List
     * @throws Exception
     */
    public List<String> getLimsRequestIdsByTimestamp(Date startTimestamp, Date endTimestamp)
            throws Exception {
        String requestUrl = limsBaseUrl + limsRequestDeliveriesEndpoint
                + String.valueOf(startTimestamp.getTime());
        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        ResponseEntity responseEntity = restTemplate.exchange(requestUrl,
                HttpMethod.GET, requestEntity, Object.class);
        List<Map> response = mapper.readValue(
                mapper.writeValueAsString(responseEntity.getBody()), List.class);

        // if endtimestamp is provided then use it to filter the response results
        // otherwise simply return set of request ids from response as list of strings
        List<String> requestIds = new ArrayList<>();
        for (Map m : response) {
            Long deliveryDate = (Long) m.get("deliveryDate");
            Date deliveryDateTimestamp = new Date(deliveryDate);
            if (endTimestamp != null && deliveryDateTimestamp.after(endTimestamp)) {
                LOG.debug("Request delivery date not within specified range, it will be skipped: "
                        + m.get("request") + ", date: " + DATE_FORMAT.format(endTimestamp));
                continue;
            }
            // only add unique request ids to list
            String requestId = (String) m.get("request");
            if (!requestIds.contains(requestId)) {
                requestIds.add(requestId);
            }
        }
        return requestIds;
    }

    /**
     * Calls LimsRest and returns the list of samples for a given request id.
     * @param requestId
     * @return Map
     * @throws Exception
     */
    @Async("asyncLimsRequestThreadPoolTaskExecutor")
    public CompletableFuture<Map<String, Object>> getLimsRequestSamples(String requestId) throws Exception {
        String requestUrl = limsBaseUrl + limsRequestSamplesEndpoint + requestId;
        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        ResponseEntity responseEntity = restTemplate.exchange(requestUrl,
                HttpMethod.GET, requestEntity, Object.class);
        Map<String, Object> response = mapper.readValue(
                mapper.writeValueAsString(responseEntity.getBody()), Map.class);
        LOG.debug("Response from LIMS:\n" + mapper.writeValueAsString(responseEntity.getBody()));
        return CompletableFuture.completedFuture(response);
    }

    /**
     * Extracts list of sample ids as strings from the LIMS response.
     * @param response
     * @return
     */
    public List<String> getSampleIdsFromRequestResponse(Map<String, Object> response)
            throws JsonProcessingException {
        String samplesListJson = mapper.writeValueAsString(response.get("samples"));
        List<Map> samplesListMap = mapper.readValue(samplesListJson, List.class);
        List<String> sampleIds = new ArrayList<>();
        if (samplesListMap != null) {
            for (Map m : samplesListMap) {
                sampleIds.add((String) m.get("igoSampleId"));
            }
        }
        return sampleIds;
    }

    /**
     * Returns a list with a single sample manifest object given a sample id.
     * @param sampleId
     * @return List
     */
    @Async("asyncLimsRequestThreadPoolTaskExecutor")
    public CompletableFuture<List<Object>> getSampleManifest(String sampleId) throws Exception {
        String manifestUrl = limsBaseUrl + limsSampleManifestEndpoint + sampleId;
        LOG.debug("Sending request for sample manifest with url:" + manifestUrl);

        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        Object[] sampleManifest = null;
        try {
            ResponseEntity responseEntity = restTemplate.exchange(manifestUrl,
                HttpMethod.GET, requestEntity, Object[].class);
            sampleManifest = (Object[]) responseEntity.getBody();
        } catch (HttpServerErrorException e) {
            if (e.getStatusCode().equals(HttpStatus.INTERNAL_SERVER_ERROR)) {
                LOG.error("Error encountered during attempt to fetch sample manifest for '"
                        + sampleId + "', request url: '" + manifestUrl + "'", e);
            }
        }
        return CompletableFuture.completedFuture(Arrays.asList(sampleManifest));
    }

    /**
     * Returns rest template that by passes SSL cert check.
     * @return RestTemplate
     * @throws Exception
     */
    private RestTemplate getRestTemplate() throws Exception {
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
        HostnameVerifier hostnameVerifier = (s, sslSession) -> true;
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }

    /**
     * Returns request entity.
     * @return HttpEntity
     */
    private HttpEntity getRequestEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(limsUsername, limsPassword);
        return new HttpEntity<Object>(headers);
    }

    public Map<String, List<String>> getLimsRequestErrors() {
        return limsRequestErrors;
    }

    public void setLimsRequestErrors(Map<String, List<String>> limsRequestErrors) {
        this.limsRequestErrors = limsRequestErrors;
    }

    /**
     * Update map with lims request errors.
     * @param requestId
     * @param error
     */
    public void updateLimsRequestErrors(String requestId, String error) {
        List<String> errors = limsRequestErrors.getOrDefault(requestId, new ArrayList<>());
        errors.add(error);
        this.limsRequestErrors.put(requestId, errors);
    }

    public void printFailedRequestSamplesSummary() {
        System.out.println(generateFailedRequestErrorsSummary());
    }

    /**
     * Generates message for failed request samples manifest report.
     * @return
     */
    private String generateFailedRequestErrorsSummary() {
        StringBuilder builder = new StringBuilder("\nERROR SUMMARY REPORT BY REQUEST\n");
        builder.append("\t--> Total number of requests with errors: ")
                .append(limsRequestErrors.size())
                .append("\n");
        for (String requestId : limsRequestErrors.keySet()) {
            List<String> errors = limsRequestErrors.get(requestId);
            builder.append("\nRequest: ")
                    .append(requestId)
                    .append(", errors: ")
                    .append(errors.size());
            for (String er : errors) {
                builder.append("\n\t")
                        .append(er)
                        .append("\n");
            }
        }
        return builder.toString();
    }
}
