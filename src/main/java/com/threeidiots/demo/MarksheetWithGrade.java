package com.threeidiots.demo;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class MarksheetWithGrade {
    public static void main(String[] args){
        PipelineOptions options=PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        List<Integer> scores = Arrays.asList(450,120,230,480,360,470,530);
        pipeline.apply(Create.of(scores))           //PCollection<Integer>
            .apply(new BuildGrade())
            .apply(TextIO.write()                   //PCollection<String>
                .to("score-grade-card")
                .withSuffix("csv")
                .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    public static class BuildGrade extends PTransform<PCollection<Integer>,PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<Integer> input) {
            PCollection<String> output = input.apply(ParDo.of(new FindGrade()));
            return output;
        }
    }

    public static class FindGrade extends DoFn<Integer, String> {
        @ProcessElement
        public void ProcessElement(@Element Integer score, OutputReceiver<String> ret) {
            var garde="Unknown";
            if(score >=500 && score<=600){
                garde = "A";
            }
            else if (score >= 400 && score < 500){
                garde = "B";
            }
            else if (score >= 300 && score < 400){
                garde = "C";
            }
            else if (score >= 200 && score < 300){
                garde = "D";
            }
            else if (score < 200){
                garde = "E";
            }
            ret.output(String.format("%d,%s", score, garde));
        }
    }
}
