package org.mskcc.cmo.publisher.pipeline.metadb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author ochoaa
 */
@Component
public class MetadbServiceUtil {
    private final ObjectMapper mapper = new ObjectMapper();
    @Value("${metadb.base_url}")
    private String metadbBaseUrl;
    @Value("${metadb.request_endpoint}")
    private String metadbRequestEndpoint;

    /**
     * Given a requestID, returns response from Metadb web service.
     * @param requestId
     * @return String
     * @throws Exception
     */
    public String getRequestById(String requestId) throws Exception {
        String requestUrl = metadbBaseUrl + metadbRequestEndpoint + requestId;
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = getRequestEntity();
        ResponseEntity responseEntity = restTemplate.exchange(requestUrl,
                HttpMethod.GET, requestEntity, Object.class);
        String requestJsonString = mapper.writeValueAsString(responseEntity.getBody());
        return requestJsonString;
    }

    /**
     * Returns request entity.
     * @return HttpEntity
     */
    private HttpEntity getRequestEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<Object>(headers);
    }

}
