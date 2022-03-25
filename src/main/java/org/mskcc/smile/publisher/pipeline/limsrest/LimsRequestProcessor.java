package org.mskcc.smile.publisher.pipeline.limsrest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    @Autowired
    private LimsRequestUtil limsRestUtil;

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
        List<String> sampleIds = limsRestUtil.getSampleIdsFromRequestResponse(requestResponse);

        if (!requestResponse.containsKey("samples") || sampleIds == null || sampleIds.isEmpty()) {
            LOG.error("Parsing request with no samples" + requestId);
            limsRestUtil.updateLimsRequestErrors(requestId, "Request JSON does not contain 'samples'");
            return null;
        }

        // get sample manifest for each sample id
        List<String> samplesWithErrors = new ArrayList<>();
        List<Object> sampleManifestList = new ArrayList<>();
        for (String sampleId : sampleIds) {
            try {
                CompletableFuture<List<Object>> manifest = limsRestUtil.getSampleManifest(sampleId);
                if (manifest != null) {
                    sampleManifestList.addAll(manifest.get());
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
        if (requestResponse.containsKey("deliveryDate")) {
            requestResponse.remove("deliveryDate");
        }

        // update request response with sample manifests fetched
        // and add project id as well for SMILE
        String projectId = requestId.split("_")[0];
        requestResponse.put("projectId", projectId);
        requestResponse.put("samples", sampleManifestList);
        return requestResponse;
    }

}
