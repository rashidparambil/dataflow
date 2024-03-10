package com.threeidiots.demo;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

public class MarkSheet {
    public static void main(String[] args){
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        List<String> scores = Arrays.asList("450", "230", "150", "562","220","210");
        pipeline.apply(Create.of(scores))
            .apply(TextIO.write()
                    .to("score_card")
                    .withSuffix(".csv")
                    .withNumShards(1));
        pipeline.run().waitUntilFinish();
    }
}
