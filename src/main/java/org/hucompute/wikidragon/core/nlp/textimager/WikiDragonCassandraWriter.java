package org.hucompute.wikidragon.core.nlp.textimager;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import org.apache.uima.util.XMLSerializer;
import org.hucompute.wikidragon.core.nlp.annotation.WikiTextSpan;
import org.xml.sax.SAXException;

public class WikiDragonCassandraWriter extends JCasConsumer_ImplBase {

    private static Logger logger = LogManager.getLogger(WikiDragonCassandraCollectionReader.class);

    public static final String PARAM_KEYSPACE = "keyspace";
    @ConfigurationParameter(name=PARAM_KEYSPACE, mandatory=true)
    private String keyspace;

    public static final String PARAM_DBNAME = "dbname";
    @ConfigurationParameter(name=PARAM_DBNAME, mandatory=true)
    private String dbname;

    public static final String PARAM_USER = "user";
    @ConfigurationParameter(name=PARAM_USER, mandatory=true)
    private String user;

    public static final String PARAM_PASSWORD = "password";
    @ConfigurationParameter(name=PARAM_PASSWORD, mandatory=true)
    private String password;

    public static final String PARAM_CONTACTHOSTS = "contactHosts";
    @ConfigurationParameter(name=PARAM_CONTACTHOSTS, mandatory=true)
    private String[] contactHosts;

    private Cluster cluster;
    private Session session;
    private PreparedStatement preparedStatement;
    private long written = 0;

    private void init() throws CollectionException, IOException {
        Cluster.Builder lBuilder = Cluster.builder();
        for (String lHost:contactHosts) {
            lBuilder.addContactPoint(lHost);
        }
        lBuilder.withCredentials(user, password);
        lBuilder.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.ANY);
        lBuilder.getConfiguration().getSocketOptions().setConnectTimeoutMillis(30000); // Default: 5000
        lBuilder.getConfiguration().getSocketOptions().setReadTimeoutMillis(30000); // Default 12000
        cluster = lBuilder.build();
        session = cluster.connect();
        session.execute("use "+keyspace);
        preparedStatement = session.prepare("UPDATE wikitextspannlp SET xmi=?, xmilen=?, processed=True WHERE dbname=? AND raw=?");
        written = 0;
    }

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        try {
            init();
        }
        catch (CollectionException e) {
            throw new ResourceInitializationException(e);
        }
        catch (IOException e) {
            throw new ResourceInitializationException(e);
        }
    }

    @Override
    public void batchProcessComplete() throws AnalysisEngineProcessException {
        super.batchProcessComplete();
        close();
    }

    @Override
    public void collectionProcessComplete() throws AnalysisEngineProcessException {
        super.collectionProcessComplete();
        close();
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        Collection<WikiTextSpan> lDocumentMetaDataCollection = JCasUtil.select(jCas, WikiTextSpan.class);
        if (lDocumentMetaDataCollection.size() == 0) {
            logger.warn("Received JCas without a WikiTextSpan - Ignoring it");
        }
        else if (lDocumentMetaDataCollection.size() > 1) {
            logger.warn("Received JCas with multiple WikiTextSpans - Ignoring it");
        }
        else {
            String lUID = lDocumentMetaDataCollection.iterator().next().getUid();
            ByteArrayOutputStream lOutput = new ByteArrayOutputStream();
            try {
                XmiCasSerializer.serialize(jCas.getCas(), lOutput);
            }
            catch(SAXException e) {
                throw new AnalysisEngineProcessException(e);
            }
            String lXMI = new String(lOutput.toByteArray());
            ResultSet lResultSet = session.execute(preparedStatement.bind(lXMI, lXMI.getBytes(Charset.forName("UTF-8")).length, dbname, lUID));
            if (!lResultSet.wasApplied()) {
                throw new AnalysisEngineProcessException(new IOException("Update could not be applied"));
            }
            logger.info("Written Documents: "+(++written));
        }
    }

    @Override
    public void destroy() {
        close();
    }

    public void close() {
        if (session != null) {
            logger.info("Closing Cassandra Session of WikiDragonCassandraWriter...");
            session.close();
            session = null;
            logger.info("Closing Cassandra Session of WikiDragonCassandraWriter... Done");
        }
        if (cluster != null) {
            logger.info("Closing Cassandra Cluster of WikiDragonCassandraWriter...");
            cluster.close();
            cluster = null;
            logger.info("Closing Cassandra Cluster of WikiDragonCassandraWriter... Done");
        }
    }

}
