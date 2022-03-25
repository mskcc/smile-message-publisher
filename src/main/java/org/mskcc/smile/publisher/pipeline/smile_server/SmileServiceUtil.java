package org.mskcc.smile.publisher.pipeline.smile_server;

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
public class SmileServiceUtil {
    private final ObjectMapper mapper = new ObjectMapper();
    @Value("${smile.base_url}")
    private String smileBaseUrl;
    @Value("${smile.request_endpoint}")
    private String smileRequestEndpoint;

    /**
     * Given a requestID, returns response from the SMILE web service.
     * @param requestId
     * @return String
     * @throws Exception
     */
    public String getRequestById(String requestId) throws Exception {
        String requestUrl = smileBaseUrl + smileRequestEndpoint + requestId;
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
