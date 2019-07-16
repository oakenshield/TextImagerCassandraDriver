package org.hucompute.wikidragon.core.nlp.textimager;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.xml.sax.SAXException;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * WikiDragonCassandraCollectionReader
 */
public class WikiDragonCassandraCollectionReader extends CasCollectionReader_ImplBase implements Progress {

    private static Logger logger = LogManager.getLogger(WikiDragonCassandraCollectionReader.class);

    public enum ProcessingState {PROCESSED, UNPROCESSED, ANY};

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

    public static final String PARAM_PROCESSINGSTATE = "processingState";
    @ConfigurationParameter(name=PARAM_PROCESSINGSTATE, mandatory=true)
    private ProcessingState processingState;

    public static final String PARAM_SKIPZEROLENGTH = "skipZeroLength";
    @ConfigurationParameter(name=PARAM_SKIPZEROLENGTH, mandatory=true)
    private boolean skipZeroLength;

    private ResultSet resultSet;
    private String[] next = null;
    private String language;
    private Cluster cluster;
    private Session session;
    private long documentsRead;
    private long relevantDocumentsRead;
    private long prefetchDocumentsRead;
    private long prefetchRelevantDocumentsRead;
    private long documentsTotal;
    private long relevantDocumentsTotal;
    private boolean currentlyInComputePooledDocumentsRelevant = false;
    
    @Override
    public void initialize(UimaContext aContext) throws ResourceInitializationException {
    	super.initialize(aContext);
        try {
			init();
		}
        catch (CollectionException | IOException e) {
			throw new ResourceInitializationException(e);
		}
    }
    
    private void init() throws CollectionException, IOException{
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
        documentsRead = 0;
        relevantDocumentsRead = 0;
        prefetchDocumentsRead = 0;
        prefetchRelevantDocumentsRead = 0;
        computePooledDocumentsRelevant();
        resultSet = session.execute("select dbname,raw,textlen,xmilen,processed,xmi from wikitextspannlp");
        documentsRead = 0;
        relevantDocumentsRead = 0;
        prefetchDocumentsRead = 0;
        prefetchRelevantDocumentsRead = 0;
        prefetch();
    }

    private void computePooledDocumentsRelevant() throws IOException, CollectionException {
        currentlyInComputePooledDocumentsRelevant = true;
        session.execute("use "+keyspace);
        resultSet = session.execute("select dbname,raw,textlen,xmilen,processed from wikitextspannlp");
        prefetch();
        long lLastTime = System.currentTimeMillis();
        documentsTotal = 0;
        relevantDocumentsTotal = 0;
        while (hasNext()) {
            getNext(null);
            relevantDocumentsTotal++;
            if (System.currentTimeMillis() - lLastTime >= 1000) {
                lLastTime = System.currentTimeMillis();
                logger.info("Documents Read: "+documentsTotal+", Documents Relevant: "+relevantDocumentsTotal);
            }
        }
        logger.info("Documents Read: "+documentsTotal+", Documents Relevant: "+relevantDocumentsTotal);
        currentlyInComputePooledDocumentsRelevant = false;
    }

    private boolean accept(Row pRow) {
        if (pRow != null) {
            String lDBName = pRow.getString(0);
            String lRaw = pRow.getString(1);
            int lTextLengthBytes = pRow.getInt(2);
            boolean lProcessed = pRow.getBool(4);
            if (lDBName.equals(dbname)) {
                if ((lTextLengthBytes > 0) || !skipZeroLength) {
                    switch (processingState) {
                        case UNPROCESSED: {
                            if (!lProcessed) return true;
                            break;
                        }
                        case PROCESSED: {
                            if (lProcessed) return true;
                            break;
                        }
                        case ANY: {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private void prefetch() {
        boolean lFoundOne = false;
        next = null;
        while (!resultSet.isExhausted() && !lFoundOne) {
            Row lRow = resultSet.one();
            if (accept(lRow)) {
                // dbname,raw,textlen,xmilen,processed,xmi
                String lKey = lRow.getString(1);
                String lXMILength = Integer.toString(lRow.getInt(3));
                String lXMI = resultSet.getColumnDefinitions().size() == 6 ? lRow.getString(5) : null;
                if ((lXMI != null) && (language == null)) {
                    int lLangIndex = lXMI.indexOf(" language=\"");
                    if (lLangIndex >= 0) {
                        language = lXMI.substring(lLangIndex+11, lXMI.indexOf('\"', lLangIndex+11));
                    }
                }
                lFoundOne = true;
                next = new String[]{lKey, lXMI, lXMILength};
                prefetchRelevantDocumentsRead++;
            }
            prefetchDocumentsRead++;
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
        super.close();
        if (session != null) {
            logger.info("Closing Cassandra Session of WikiDragonCassandraCollectionReader...");
            session.close();
            session = null;
            logger.info("Closing Cassandra Session of WikiDragonCassandraCollectionReader... Done");
        }
        if (cluster != null) {
            logger.info("Closing Cassandra Cluster of WikiDragonCassandraCollectionReader...");
            cluster.close();
            cluster = null;
            logger.info("Closing Cassandra Cluster of WikiDragonCassandraCollectionReader... Done");
        }
    }

    @Override
    public void getNext(CAS cas) throws IOException, CollectionException {
        documentsRead = prefetchDocumentsRead;
        relevantDocumentsRead = prefetchRelevantDocumentsRead;
        if (next != null) {
            byte[] lBytes = next[1] != null ? next[1].getBytes(Charset.forName("UTF-8")) : null;
            prefetch();
            if (cas != null) {
                try {
                    XmiCasDeserializer.deserialize(new ByteArrayInputStream(lBytes), cas, false);
                    Collection<DocumentMetaData> lDocumentMetaDataCollection = JCasUtil.select(cas.getJCas(), DocumentMetaData.class);
                    // Patch DocumentMetaData.DocumentTitle and DocumentMetaData.DocumentId if they are missing
                    if (lDocumentMetaDataCollection.size() > 1) {
                        throw new CollectionException(new IOException("Count of DocumentMetaData is > 1 : "+lDocumentMetaDataCollection.size()));
                    }
                    else if (lDocumentMetaDataCollection.size() == 1) {
                        DocumentMetaData lDocumentMetaData = lDocumentMetaDataCollection.iterator().next();
                        if ((lDocumentMetaData.getDocumentTitle() == null) || (lDocumentMetaData.getDocumentTitle().length() == 0)) {
                            lDocumentMetaData.setDocumentTitle(documentsRead + ".xmi");
                        }
                        if ((lDocumentMetaData.getDocumentId() == null) || (lDocumentMetaData.getDocumentId().length() == 0)) {
                            lDocumentMetaData.setDocumentId(documentsRead + ".xmi");
                        }
                    }
                    else {
                        DocumentMetaData lDocumentMetaData = new DocumentMetaData(cas.getJCas(), 0, cas.getDocumentText().length());
                        lDocumentMetaData.setDocumentTitle(documentsRead + ".xmi");
                        lDocumentMetaData.setDocumentId(documentsRead + ".xmi");
                    }
                }
                catch (CASException e) {
                    throw new IOException("Invalid XMI: " + e.getMessage(), e);
                }
                catch (SAXException e) {
                    throw new IOException("Invalid XMI: " + e.getMessage(), e);
                }
            }
        }
        else {
            if (!currentlyInComputePooledDocumentsRelevant) close();
        }
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        if (next != null) {
            return true;
        }
        else {
            if (!currentlyInComputePooledDocumentsRelevant) close();
            return false;
        }
    }

    @Override
    public Progress[] getProgress() {
        return new Progress[]{this};
    }

    @Override
    public long getCompleted() {
        return relevantDocumentsRead;
    }

    @Override
    public long getTotal() {
        return relevantDocumentsTotal;
    }

    @Override
    public String getUnit() {
        return Progress.ENTITIES;
    }

    @Override
    public boolean isApproximate() {
        return false;
    }

}
