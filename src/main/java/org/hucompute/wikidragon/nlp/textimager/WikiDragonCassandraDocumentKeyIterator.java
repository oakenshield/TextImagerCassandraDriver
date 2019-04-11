package org.hucompute.wikidragon.nlp.textimager;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.io.ResourceCollectionReaderBase;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WikiDragonCassandraDocumentKeyIterator implements AutoCloseable, Iterator<String> {

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
    private String next = null;
    private boolean skipZeroLength;
    private int documentsRead = 0;

    public WikiDragonCassandraDocumentKeyIterator(String pKeyspace, String pUser, String pPassword, String[] pContactHosts, String pDBName, TextImagerCassandraDriver.ProcessingState pProcessingState, boolean pSkipZeroLength) {
        keyspace = pKeyspace;
        user = pUser;
        password = pPassword;
        dbname = pDBName;
        contactHosts = pContactHosts;
        processingState = pProcessingState;
        skipZeroLength = pSkipZeroLength;
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
        Statement lStatement = null;
        switch (processingState) {
            case ANY: {
                lStatement = session.prepare("SELECT xmi FROM "+keyspace+".wikitextspannlp WHERE dbname=?").bind(dbname);
                break;
            }
            case PROCESSED: {
                lStatement = session.prepare("SELECT xmi FROM "+keyspace+".wikitextspannlp WHERE dbname=? AND processed=true ALLOW FILTERING").bind(dbname);
                break;
            }
            case UNPROCESSED: {
                lStatement = session.prepare("SELECT xmi FROM "+keyspace+".wikitextspannlp WHERE dbname=? AND processed=false ALLOW FILTERING").bind(dbname);
                break;
            }
        }
        resultSet = session.execute(lStatement);
        prefetch();
    }

    private void prefetch() {
        boolean lFoundOne = false;
        next = null;
        while (!resultSet.isExhausted() && !lFoundOne) {
            Row lRow = resultSet.one();
            if (lRow != null) {
                String lXMI = lRow.getString(0);
                if (lXMI != null) {
                    if (!skipZeroLength || (lXMI.contains("sofaString=\"") && !lXMI.contains("sofaString=\"\""))) {
                        lFoundOne = true;
                        next = lXMI;
                        documentsRead++;
                    }
                }
            }
        }
    }

    public int getDocumentsRead() {
        return documentsRead;
    }

    @Override
    public void close() throws IOException {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    @Override
    public String next() {
        String lResult = next;
        prefetch();
        return lResult;
    }

    @Override
    public boolean hasNext(){
        return next != null;
    }

}
