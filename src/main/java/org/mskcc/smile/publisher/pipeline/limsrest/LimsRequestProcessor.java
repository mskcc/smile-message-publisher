package org.mskcc.smile.publisher.pipeline.limsrest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;



/**
 *
 * @author ochoaa
 */
public class LimsRequestProcessor implements ItemProcessor<String, Map<String, Object>> {
    private static final Log LOG = LogFactory.getLog(LimsRequestProcessor.class);

    @Value("#{jobParameters[cmoRequestsFilter]}")
    private Boolean cmoRequestsFilter;

    @Value("#{jobParameters[igoSampleIds]}")
    private String igoSampleIds;

    @Autowired
    private LimsRequestUtil limsRestUtil;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Object> process(String requestId) throws Exception {
        CompletableFuture<Map<String, Object>> futureRequestResponse =
                limsRestUtil.getLimsRequestSamples(requestId);
        Map<String, Object> requestResponse = futureRequestResponse.get();
        // if filtering by cmo requests only then return null if request is not a cmo request
        // if the field is not available in the json response then set default to false
        if (cmoRequestsFilter) {
            Boolean isCmoRequest = (Boolean) requestResponse.getOrDefault("isCmoRequest", Boolean.FALSE);
            if (!isCmoRequest) {
                LOG.info("Skipping non-CMO request '" + requestId + "'");
                limsRestUtil.updateLimsRequestErrors(requestId, "Non-CMO request");
                return null;
            }
        }
        Map<String, Map<String, Object>> samples
                = limsRestUtil.getSamplesFromRequestResponse(requestResponse);

        if (!requestResponse.containsKey("samples") || samples == null || samples.isEmpty()) {
            LOG.error("Parsing request with no samples" + requestId);
            limsRestUtil.updateLimsRequestErrors(requestId, "Request JSON does not contain 'samples'");
            return null;
        }

        // if igoSampleIds provided then limit set of samples to get data for to only those
        Set<String> igoSamplesToFetch;
        if (!StringUtils.isBlank(igoSampleIds)) {
            igoSamplesToFetch = new HashSet<>(Arrays.asList(igoSampleIds.split(",")));
        } else {
            igoSamplesToFetch = samples.keySet();
        }

        // get sample manifest for each sample id
        List<String> samplesWithErrors = new ArrayList<>();
        List<Object> sampleManifestList = new ArrayList<>();
        for (String sampleId : igoSamplesToFetch) {
            try {
                CompletableFuture<List<Object>> manifest = limsRestUtil.getSampleManifest(sampleId);
                if (manifest != null) {
                    Object smObj = manifest.get().get(0);
                    Map<String, Object> sampleManifest = mapper.convertValue(smObj, Map.class);
                    Map<String, Object> sampleRequestMap = samples.get(sampleId);
                    sampleManifest.put("igoComplete", sampleRequestMap.get("igoComplete"));
                    sampleManifest.put("sampleStatus", sampleRequestMap.get("sampleStatus"));
                    sampleManifestList.add(sampleManifest);
                } else {
                    samplesWithErrors.add(sampleId);
                }
            } catch (Exception e) {
                samplesWithErrors.add(sampleId);
                LOG.warn("Encountered error during attempt to fetch sample manifest for sample: "
                        + sampleId, e);
            }
        }
        if (!samplesWithErrors.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Errors during sample manifest fetch: ")
                    .append(StringUtils.join(samplesWithErrors, ", "));
            limsRestUtil.updateLimsRequestErrors(requestId, builder.toString());
        }

        // update request response with sample manifests fetched
        // and add project id as well for SMILE
        String projectId = requestId.split("_")[0];
        requestResponse.put("projectId", projectId);
        requestResponse.put("samples", sampleManifestList);
        return requestResponse;
    }

}
