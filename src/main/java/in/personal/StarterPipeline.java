/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.personal;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);


    public static void main(String[] args) {

        WordCountOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(WordCountOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read lines from the input specified", TextIO.read().from(options.getInput()))
                .apply("Count words ",new CountWords())
                .apply("Format the output to String",ParDo.of(new FormatAsTextFn()))
                .apply("Write the data to the given location",TextIO.write().to(options.getOutput()));

        pipeline.run();
    }

    static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {

        @ProcessElement
        public void processElement(ProcessContext context){
            context.output(context.element().getKey() + " : " + context.element().getValue());
        }
    }


    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {

            // Convert line of words to individual words
            PCollection<String> words = input.apply(ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }
    }


    static class ExtractWordsFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            if (!context.element().trim().isEmpty()) {
                String[] words = context.element().split("[^A-Za-z]+");

                for (String word : words) {
                    if (!word.isEmpty()) {
                        context.output(word);
                    }
                }
            }
        }
    }

    public interface WordCountOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
        String getInput();

        void setInput(String value);

        @Description("Path of the file to write to")
        @Default.InstanceFactory(OutputFactory.class)
        String getOutput();

        void setOutput(String value);


        /**
         * Returns gs://$(YOUR_STAGING_DIRECTORY)/count.txt as the default destination
         */
        class OutputFactory implements DefaultValueFactory<String> {

            @Override
            public String create(PipelineOptions pipelineOptions) {
                DataflowPipelineOptions options = pipelineOptions.as(DataflowPipelineOptions.class);
                if (options.getStagingLocation() != null) {
                    return GcsPath.fromUri(options.getStagingLocation()).resolve("counts.txt").toString();
                } else {
                    throw new IllegalArgumentException("Must specify --output or --stagingLocation");
                }
            }
        }
    }
}

