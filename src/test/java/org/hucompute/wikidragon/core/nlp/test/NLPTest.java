package org.hucompute.wikidragon.core.nlp.test;

import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordPosTagger;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.hucompute.wikidragon.core.nlp.textimager.WikiDragonCassandraCollectionReader;
import org.hucompute.wikidragon.core.nlp.textimager.WikiDragonCassandraWriter;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class NLPTest {

    public static void main(String[] args) throws Exception {
        CollectionReader reader = CollectionReaderFactory.createReader(
                WikiDragonCassandraCollectionReader.class,
                WikiDragonCassandraCollectionReader.PARAM_KEYSPACE,"simplewikitest",
                WikiDragonCassandraCollectionReader.PARAM_USER,"cassandra",
                WikiDragonCassandraCollectionReader.PARAM_PASSWORD,"wkZjeCNH",
                WikiDragonCassandraCollectionReader.PARAM_CONTACTHOSTS,new String[]{"141.2.108.194"},
                WikiDragonCassandraCollectionReader.PARAM_DBNAME,"simplewiki",
                WikiDragonCassandraCollectionReader.PARAM_PROCESSINGSTATE, WikiDragonCassandraCollectionReader.ProcessingState.UNPROCESSED,
                WikiDragonCassandraCollectionReader.PARAM_SKIPZEROLENGTH, true
        );

        AnalysisEngineDescription writer = createEngineDescription(WikiDragonCassandraWriter.class,
                WikiDragonCassandraWriter.PARAM_KEYSPACE,"simplewikitest",
                WikiDragonCassandraWriter.PARAM_USER,"cassandra",
                WikiDragonCassandraWriter.PARAM_PASSWORD,"wkZjeCNH",
                WikiDragonCassandraWriter.PARAM_CONTACTHOSTS,new String[]{"141.2.108.194"},
                WikiDragonCassandraWriter.PARAM_DBNAME,"simplewiki"
        );

        AggregateBuilder builder = new AggregateBuilder();
        builder.add(createEngineDescription(StanfordPosTagger.class));
        builder.add(writer);
        SimplePipeline.runPipeline(reader, builder.createAggregate());
    }

}
