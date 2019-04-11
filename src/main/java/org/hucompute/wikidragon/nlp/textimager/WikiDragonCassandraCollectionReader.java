package org.hucompute.wikidragon.nlp.textimager;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.io.ResourceCollectionReaderBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
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

public class WikiDragonCassandraCollectionReader extends ResourceCollectionReaderBase implements AutoCloseable, Progress {

    private static Logger logger = LogManager.getLogger(WikiDragonCassandraCollectionReader.class);

    protected ConsistencyLevel secureConsistencyLevel = ConsistencyLevel.ALL;
    protected ConsistencyLevel defaultWriteConsistencyLevel = ConsistencyLevel.ANY;
    protected ConsistencyLevel defaultReadConsistencyLevel = ConsistencyLevel.ONE;

    private String keyspace;
    private String dbname;
    private String user;
    private String password;
    private String[] contactHosts;
    private Cluster cluster;
    private Session session;
    private TextImagerCassandraDriver.ProcessingState processingState;
    private ResultSet resultSet;
    private String[] next = null;
    private boolean skipZeroLength;
    private boolean poolDocuments;
    private int poolDocumentsMaxBytes;
    private int pooledDocumentsRead = 0;
    private int pooledDocumentsRelevant = 0;
    private int documentsRead = 0;
    private int documentsSkippedByLog = 0;
    private File logFile;
    private DB logDb;
    private Set<String> logSet;
    private String language;

    public WikiDragonCassandraCollectionReader(String pKeyspace, String pUser, String pPassword, String[] pContactHosts, String pDBName, TextImagerCassandraDriver.ProcessingState pProcessingState, boolean pSkipZeroLength, boolean pPoolDocuments, int pPoolDocumentsMaxBytes, File pLogFile) throws IOException, CollectionException {
        keyspace = pKeyspace;
        user = pUser;
        password = pPassword;
        dbname = pDBName;
        contactHosts = pContactHosts;
        processingState = pProcessingState;
        skipZeroLength = pSkipZeroLength;
        poolDocuments = pPoolDocuments;
        poolDocumentsMaxBytes = pPoolDocumentsMaxBytes;
        logFile = pLogFile;
        if (logFile != null) {
            logDb = DBMaker.fileDB(logFile).closeOnJvmShutdown().transactionEnable().make();
            logSet = logDb.hashSet("keys", Serializer.STRING).createOrOpen();
        }
        Cluster.Builder lBuilder = Cluster.builder();
        for (String lHost:contactHosts) {
            lBuilder.addContactPoint(lHost);
        }
        lBuilder.withCredentials(user, password);
        lBuilder.getConfiguration().getQueryOptions().setConsistencyLevel(defaultWriteConsistencyLevel);
        lBuilder.getConfiguration().getSocketOptions().setConnectTimeoutMillis(30000); // Default: 5000
        lBuilder.getConfiguration().getSocketOptions().setReadTimeoutMillis(30000); // Default 12000
        cluster = lBuilder.build();
        session = cluster.connect();
        pooledDocumentsRelevant = computePooledDocumentsRelevant();
        resultSet = session.execute("select dbname,raw,textlen,xmilen,processed,xmi from wikitextspannlp");
        prefetch();
    }

    private int computePooledDocumentsRelevant() throws IOException, CollectionException {
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
                logger.info("Atomic Docs Read: "+documentsRead+", Atomic Docs Skipped by Log: "+documentsSkippedByLog+", RelevantPooledDocs: "+lResult);
            }
        }
        logger.info("Atomic Docs Read: "+documentsRead+", Atomic Docs Skipped by Log: "+documentsSkippedByLog+", RelevantPooledDocs: "+pooledDocumentsRead);
        pooledDocumentsRead = 0;
        pooledDocumentsRelevant = 0;
        documentsRead = 0;
        documentsSkippedByLog = 0;
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
                            if ((logSet == null) || !logSet.contains(lRaw)) {
                                if (!lProcessed) return true;
                            }
                            else {
                                documentsSkippedByLog++;
                            }
                            break;
                        }
                        case PROCESSED: {
                            if ((logSet == null) || !logSet.contains(lRaw)) {
                                if (lProcessed) return true;
                            }
                            else {
                                documentsSkippedByLog++;
                            }
                            break;
                        }
                        case ANY: {
                            if ((logSet == null) || !logSet.contains(lRaw)) {
                                return true;
                            }
                            else {
                                documentsSkippedByLog++;
                            }
                            break;
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

    /**
     * Number of atomic documents which may fit the requirements but have been skipped because they are already in the log
     * @return
     */
    public int getDocumentsSkippedByLog() {
        return documentsSkippedByLog;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (logDb != null) logDb.close();
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    @Override
    public void getNext(CAS cas) throws IOException, CollectionException {
        if (next != null) {
            Set<String> lDeliveredKeys = new HashSet<>();
            byte[] lBytes = null;
            if (poolDocuments) {
                List<FastDocument> lDocuments = new ArrayList<>();
                int lBytesLength = Integer.parseInt(next[2]);
                if (next[1] != null) lDocuments.add(FastDocument.fromXMI(next[1]));
                lDeliveredKeys.add(next[0]);
                prefetch();
                pooledDocumentsRead++;
                while ((lBytesLength <= poolDocumentsMaxBytes) && (next != null)) {
                    if (lBytesLength + Integer.parseInt(next[2]) <= poolDocumentsMaxBytes) {
                        lDeliveredKeys.add(next[0]);
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
                lDeliveredKeys.add(next[0]);
                prefetch();
                pooledDocumentsRead++;
            }
            if (cas != null) {
                try {
                    XmiCasDeserializer.deserialize(new ByteArrayInputStream(lBytes), cas, true);
                } catch (SAXException e) {
                    throw new IOException("Invalid XMI: " + e.getMessage(), e);
                }
                if (logSet != null) {
                    for (String lKey : lDeliveredKeys) {
                        logSet.add(lKey);
                    }
                    logDb.commit();
                }
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        return next != null;
    }

    @Override
    public String getLanguage() {
        return language;
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
