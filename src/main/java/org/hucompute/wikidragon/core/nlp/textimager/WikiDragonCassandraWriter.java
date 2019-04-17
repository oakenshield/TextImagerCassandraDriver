package org.hucompute.wikidragon.core.nlp.textimager;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import com.datastax.driver.core.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import org.apache.uima.util.XMLSerializer;
import org.xml.sax.SAXException;

public class WikiDragonCassandraWriter extends JCasConsumer_ImplBase implements AutoCloseable {

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

    public WikiDragonCassandraWriter() {
    }

    public WikiDragonCassandraWriter(String pKeyspace, String pUser, String pPassword, String[] pContactHosts, String pDBName) throws IOException, CollectionException {
        keyspace = pKeyspace;
        user = pUser;
        password = pPassword;
        dbname = pDBName;
        contactHosts = pContactHosts;
    }

    private void init() throws CollectionException, IOException {
        Cluster.Builder lBuilder = Cluster.builder();
        for (String lHost:contactHosts) {
            lBuilder.addContactPoint(lHost);
        }
        lBuilder.withCredentials(user, password);
        lBuilder.getConfiguration().getQueryOptions().setConsistencyLevel(TextImagerCassandraDriver.defaultWriteConsistencyLevel);
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
        try {
            close();
        }
        catch (IOException e) {
            throw new AnalysisEngineProcessException(e);
        }
    }

    @Override
    public void collectionProcessComplete() throws AnalysisEngineProcessException {
        super.collectionProcessComplete();
        try {
            close();
        }
        catch (IOException e) {
            throw new AnalysisEngineProcessException(e);
        }
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        ByteArrayOutputStream lOutput = new ByteArrayOutputStream();
        try {
            XmiCasSerializer.serialize(jCas.getCas(), lOutput);
        }
        catch(SAXException e) {
            throw new AnalysisEngineProcessException(e);
        }
        String lRawXMI = new String(lOutput.toByteArray());
        try {
            for (FastDocument lDocument : split(FastDocument.fromXMI(lRawXMI))) {
                List<FastAnnotation> lAnnotations = lDocument.getAnnotations(FastDocument.NS_WIKIDRAGON_URI, "WikiTextSpan", false);
                if (lAnnotations.size() == 1) {
                    String lXMI = lDocument.exportXMI();
                    String lUID = lAnnotations.get(0).getAttributeValue("uid", null);
                    ResultSet lResultSet = session.execute(preparedStatement.bind(lXMI, lXMI.getBytes(Charset.forName("UTF-8")).length, dbname, lUID));
                    if (!lResultSet.wasApplied()) {
                        throw new AnalysisEngineProcessException(new IOException("Update could not be applied"));
                    }
                }
                logger.info("Written Documents: "+(++written));
            }
        }
        catch (IOException e) {
            throw new AnalysisEngineProcessException(e);
        }
    }

    @Override
    public void destroy() {
        try {
            close();
        }
        catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
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

    public static List<FastDocument> split(FastDocument pFastDocument) throws AnalysisEngineProcessException {
        List<FastDocument> lResult = new ArrayList<>();
        for (FastAnnotation lWikiTextSpan:pFastDocument.getAnnotations(FastDocument.NS_WIKIDRAGON_URI, "WikiTextSpan", true)) {
            int lOffset = lWikiTextSpan.getBegin();
            FastDocument lNewDocument = new FastDocument(pFastDocument.getLanguage(), lWikiTextSpan.toString());
            List<FastAnnotation> lAnnotations = pFastDocument.getSubsumedAnnotations(lWikiTextSpan.getBegin(), lWikiTextSpan.getEnd(), true);
            int lMinOrder = Integer.MAX_VALUE;
            for (FastAnnotation lAnnotation:lAnnotations) {
                if (lAnnotation.getTypeUri().equals(FastDocument.NS_WIKIDRAGON_URI) && lAnnotation.getName().equals("HtmlTag")) {
                    int lOrder = Integer.parseInt(lAnnotation.getAttributeValue("order", null));
                    lMinOrder = Math.min(lMinOrder, lOrder);
                }
            }
            if (lMinOrder == Integer.MAX_VALUE) lMinOrder = 0;
            for (FastAnnotation lAnnotation:lAnnotations) {
                FastAnnotation lNewAnnotation = lNewDocument.addAnnotation(lAnnotation.getTypeUri(), lAnnotation.getName(), lAnnotation.getBegin()-lOffset, lAnnotation.getEnd()-lOffset);
                if (lAnnotation.getTypeUri().equals(FastDocument.NS_WIKIDRAGON_URI) && lAnnotation.getName().equals("HtmlTag")) {
                    for (Map.Entry<String, String> lEntry:lAnnotation.getAttributes().entrySet()) {
                        if (lEntry.getKey().equals("order")) {
                            lNewAnnotation.setAttributeValue(lEntry.getKey(), Integer.toString(Integer.parseInt(lEntry.getValue())-lMinOrder));
                        }
                        else {
                            lNewAnnotation.setAttributeValue(lEntry.getKey(), lEntry.getValue());
                        }
                    }
                }
                else {
                    for (Map.Entry<String, String> lEntry:lAnnotation.getAttributes().entrySet()) {
                        lNewAnnotation.setAttributeValue(lEntry.getKey(), lEntry.getValue());
                    }
                }
            }
            lResult.add(lNewDocument);
        }
        return lResult;
    }

}
