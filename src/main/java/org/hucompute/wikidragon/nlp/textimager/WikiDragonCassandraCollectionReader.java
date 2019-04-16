package org.hucompute.wikidragon.nlp.textimager;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.internal.ExtendedLogger;
import org.apache.uima.resource.CasManager;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.Progress;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class WikiDragonCassandraCollectionReader extends CasCollectionReader_ImplBase implements AutoCloseable, Progress {

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


    public static final String PARAM_PROCESSINGSTATE = "processingState";
    @ConfigurationParameter(name=PARAM_PROCESSINGSTATE, mandatory=true)
    private TextImagerCassandraDriver.ProcessingState processingState;

    public static final String PARAM_SKIPZEROLENGTH = "skipZeroLength";
    @ConfigurationParameter(name=PARAM_SKIPZEROLENGTH, mandatory=true)
    private boolean skipZeroLength;
    
    public static final String PARAM_POOLDOCUMENTS = "poolDocuments";
    @ConfigurationParameter(name=PARAM_POOLDOCUMENTS, mandatory=true)
    private boolean poolDocuments;
    
    public static final String PARAM_POOLDOCUMENTSMAXBYTES = "poolDocumentsMaxBytes";
    @ConfigurationParameter(name=PARAM_POOLDOCUMENTSMAXBYTES, mandatory=true)
    private int poolDocumentsMaxBytes;

    private ResultSet resultSet;
    private String[] next = null;
    private int pooledDocumentsRead = 0;
    private int pooledDocumentsRelevant = 0;
    private int documentsRead = 0;
    private String language;
    private Cluster cluster;
    private Session session;
    private boolean currentlyInComputePooledDocumentsRelevant = false;
    
    @Override
    public void initialize(UimaContext aContext) throws ResourceInitializationException {
    	// TODO Auto-generated method stub
    	super.initialize(aContext);
        try {
			init();
		} catch (CollectionException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private void init() throws CollectionException, IOException{
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
        pooledDocumentsRelevant = computePooledDocumentsRelevant();
        resultSet = session.execute("select dbname,raw,textlen,xmilen,processed,xmi from wikitextspannlp");
        prefetch();
    }

    public WikiDragonCassandraCollectionReader(){
    }
    
    public WikiDragonCassandraCollectionReader(String pKeyspace, String pUser, String pPassword, String[] pContactHosts, String pDBName, TextImagerCassandraDriver.ProcessingState pProcessingState, boolean pSkipZeroLength, boolean pPoolDocuments, int pPoolDocumentsMaxBytes) throws IOException, CollectionException {
        keyspace = pKeyspace;
        user = pUser;
        password = pPassword;
        dbname = pDBName;
        contactHosts = pContactHosts;
        processingState = pProcessingState;
        skipZeroLength = pSkipZeroLength;
        poolDocuments = pPoolDocuments;
        poolDocumentsMaxBytes = pPoolDocumentsMaxBytes;
        init();
    }

    private int computePooledDocumentsRelevant() throws IOException, CollectionException {
        currentlyInComputePooledDocumentsRelevant = true;
        int lResult = 0;
        session.execute("use "+keyspace);
        resultSet = session.execute("select dbname,raw,textlen,xmilen,processed from wikitextspannlp");
        prefetch();
        long lLastTime = System.currentTimeMillis();
        while (hasNext()) {
            getNext(null);
            lResult++;
            if (System.currentTimeMillis() - lLastTime >= 1000) {
                lLastTime = System.currentTimeMillis();
                logger.info("Atomic Docs Read: "+documentsRead+", RelevantPooledDocs: "+lResult);
            }
        }
        logger.info("Atomic Docs Read: "+documentsRead+", RelevantPooledDocs: "+pooledDocumentsRead);
        pooledDocumentsRead = 0;
        pooledDocumentsRelevant = 0;
        documentsRead = 0;
        currentlyInComputePooledDocumentsRelevant = false;
        return lResult;
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
            }
            documentsRead++;
        }
    }

    /**
     * Get number of pooled documents which have been newly added in this run
     * @return
     */
    public int getPooledDocumentsRead() {
        return pooledDocumentsRead;
    }

    /**
     * Get number of (atomic) documents which have been read from the database, whether they have to be considered or not
     * @return
     */
    public int getDocumentsRead() {
        return documentsRead;
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
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    @Override
    public void getNext(CAS cas) throws IOException, CollectionException {
        if (next != null) {
            byte[] lBytes = null;
            if (poolDocuments) {
                List<FastDocument> lDocuments = new ArrayList<>();
                int lBytesLength = Integer.parseInt(next[2]);
                if (next[1] != null) lDocuments.add(FastDocument.fromXMI(next[1]));
                prefetch();
                pooledDocumentsRead++;
                while ((lBytesLength <= poolDocumentsMaxBytes) && (next != null)) {
                    if (lBytesLength + Integer.parseInt(next[2]) <= poolDocumentsMaxBytes) {
                        lBytesLength += Integer.parseInt(next[2]);
                        if (next[1] != null) lDocuments.add(FastDocument.fromXMI(next[1]));
                        prefetch();
                    }
                    else {
                        break;
                    }
                }
                lBytes = lDocuments.size() > 0 ? join(lDocuments).exportXMI().getBytes(Charset.forName("UTF-8")) : null;
            }
            else {
                lBytes = next[1] != null ? next[1].getBytes(Charset.forName("UTF-8")) : null;
                prefetch();
                pooledDocumentsRead++;
            }
            if (cas != null) {
                try {
                    XmiCasDeserializer.deserialize(new ByteArrayInputStream(lBytes), cas, false);
                    DocumentMetaData docMetaData;
        			try {
        				docMetaData = DocumentMetaData.create(cas);
        	            docMetaData.setDocumentTitle(""+pooledDocumentsRead);
        	            docMetaData.setDocumentId(""+pooledDocumentsRead);
        			} catch (CASException | IllegalStateException e1) {
        				// TODO Auto-generated catch block
        				e1.printStackTrace();
        			}
                } catch (SAXException e) {
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
        return pooledDocumentsRead;
    }

    @Override
    public long getTotal() {
        return pooledDocumentsRelevant;
    }

    @Override
    public String getUnit() {
        return Progress.ENTITIES;
    }

    @Override
    public boolean isApproximate() {
        return false;
    }

    private static FastDocument join(List<FastDocument> pFastDocuments) throws IOException {
        StringBuilder lJointText = new StringBuilder();
        FastDocument lResult = new FastDocument("");
        int lOffset = 0;
        int lIndexOrderOffset = 0;
        for (FastDocument lFastDocument:pFastDocuments) {
            int lCurrentMaxOrderOffset = 0;
            if (lFastDocument == null) return null;
            lResult.setLanguage(lFastDocument.getLanguage());
            lJointText.append(lFastDocument.getText());
            for (FastAnnotation lAnnotation:lFastDocument.getAnnotations(true)) {
                FastAnnotation lNewAnnotation = lResult.addAnnotation(lAnnotation.getTypeUri(), lAnnotation.getName(), lAnnotation.getBegin() + lOffset, lAnnotation.getEnd() + lOffset);
                for (Map.Entry<String, String> lEntry:lAnnotation.getAttributes().entrySet()) {
                    lNewAnnotation.setAttributeValue(lEntry.getKey(), lEntry.getValue());
                }
                // Fix Order-Attribute of HtmlTag
                if (lNewAnnotation.getTypeUri().equals(FastDocument.NS_WIKIDRAGON_URI) && (lNewAnnotation.getName().equals("HtmlTag"))) {
                    int lOrder = Integer.parseInt(lNewAnnotation.getAttributeValue("order", null));
                    if (lOrder > lCurrentMaxOrderOffset) {
                        lCurrentMaxOrderOffset = lOrder;
                    }
                    lNewAnnotation.setAttributeValue("order", Integer.toString(lOrder+lIndexOrderOffset));
                }
            }
            lJointText.append(' '); // Add a dummy whitespace for better readability.
            lOffset = lJointText.length();
            lIndexOrderOffset += lCurrentMaxOrderOffset+1;
        }
        lResult.setText(lJointText.toString());
        return lResult;
    }
}
