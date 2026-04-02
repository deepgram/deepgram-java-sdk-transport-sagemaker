package com.deepgram.sagemaker;

import software.amazon.awssdk.regions.Region;

/**
 * Configuration for connecting to a Deepgram model hosted on SageMaker.
 */
public class SageMakerConfig {

    private final String endpointName;
    private final Region region;
    private final String contentType;
    private final String acceptType;

    private SageMakerConfig(Builder builder) {
        this.endpointName = builder.endpointName;
        this.region = builder.region;
        this.contentType = builder.contentType;
        this.acceptType = builder.acceptType;
    }

    public String endpointName() { return endpointName; }
    public Region region() { return region; }
    public String contentType() { return contentType; }
    public String acceptType() { return acceptType; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String endpointName;
        private Region region = Region.US_WEST_2;
        private String contentType = "application/octet-stream";
        private String acceptType = "application/json";

        public Builder endpointName(String endpointName) {
            this.endpointName = endpointName;
            return this;
        }

        public Builder region(Region region) {
            this.region = region;
            return this;
        }

        public Builder region(String region) {
            this.region = Region.of(region);
            return this;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder acceptType(String acceptType) {
            this.acceptType = acceptType;
            return this;
        }

        public SageMakerConfig build() {
            if (endpointName == null || endpointName.isBlank()) {
                throw new IllegalArgumentException("endpointName is required");
            }
            return new SageMakerConfig(this);
        }
    }
}
