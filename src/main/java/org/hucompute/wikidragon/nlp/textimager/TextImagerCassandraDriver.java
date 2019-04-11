package org.hucompute.wikidragon.nlp.textimager;

import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;

public class TextImagerCassandraDriver {

    public enum ProcessingState {PROCESSED, UNPROCESSED, ANY};

    private static void printSyntax() {
        System.out.println("TextImagerCassandraDriver exportfs -ks <keyspace> -u <user> -p <password> -h <host> -db <dbname> -state <all|tagged|untagged> -pkb <poolMaxKiloBytes> -d <target-dir> [-log <logfile>] [-sz]");
        System.out.println("e.g. TextImagerCassandraDriver exportfs -ks simplewiki_20190201 -u cassandra -p password -h 141.2.89.24 -db simplewiki -state untagged -pkb 1024 -d data");
        System.out.println("if -pkb is skipped or set to 0, documents wil not be pooled");
        System.out.println("-ks <keyspace>");
        System.out.println("-u <user>");
        System.out.println("-p <password>");
        System.out.println("-h <host>");
        System.out.println("-db <dbname>");
        System.out.println("-state <all|tagged|untagged>");
        System.out.println("-pkb <poolMaxKiloBytes>");
        System.out.println("-d <poolMaxKiloBytes>");
        System.out.println("-sz - skip XMI with zero length texts>");
        System.out.println("-log <logfile> - Should be used to log what as already been fetched. This allows to resume a process which may have been interrupted");
        System.out.println();
        System.out.println("TextImagerCassandraDriver countdocs -ks <keyspace> -u <user> -p <password> -h <host> -db <dbname> -state <all|tagged|untagged> -pkb <poolMaxKiloBytes> [-sz]");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) printSyntax();
        String lKeyspace = null;
        String lUser = null;
        String lPassword = null;
        String lHost = null;
        String lDBName = null;
        ProcessingState lProcessingState = null;
        boolean lSkipZero = false;
        int lPoolMaxBytes = 0;
        String lCommand = args[0];
        File lTargetDir = null;
        File lLogFile = null;
        for (int i=1; i<args.length; i++) {
            switch (args[i]) {
                case "-ks": {
                    lKeyspace = args[++i];
                    break;
                }
                case "-u": {
                    lUser = args[++i];
                    break;
                }
                case "-p": {
                    lPassword = args[++i];
                    break;
                }
                case "-h": {
                    lHost = args[++i];
                    break;
                }
                case "-db": {
                    lDBName = args[++i];
                    break;
                }
                case "-state": {
                    switch (args[++i]) {
                        case "all": {
                            lProcessingState = ProcessingState.ANY;
                            break;
                        }
                        case "untagged": {
                            lProcessingState = ProcessingState.UNPROCESSED;
                            break;
                        }
                        case "tagged": {
                            lProcessingState = ProcessingState.PROCESSED;
                            break;
                        }
                    }
                    break;
                }
                case "-pkb": {
                    lPoolMaxBytes = Integer.parseInt(args[++i]) * 1024;
                    break;
                }
                case "-d": {
                    lTargetDir = new File(args[++i]);
                    break;
                }
                case "-log": {
                    lLogFile = new File(args[++i]);
                    break;
                }
                case "-sz": {
                    lSkipZero = true;
                    break;
                }
            }
        }
        boolean lPoolDocs = lPoolMaxBytes > 0;
        switch (lCommand) {
            case "exportfs": {
                JCas lJcas = JCasFactory.createJCas("HtmlTagTypeSystemDescriptor");
                try (WikiDragonCassandraCollectionReader lReader = new WikiDragonCassandraCollectionReader(lKeyspace, lUser, lPassword, new String[]{lHost}, lDBName, lProcessingState, lSkipZero, lPoolDocs, lPoolMaxBytes, lLogFile)) {
                    int lDocCounter = 1;
                    while (lReader.hasNext()) {
                        lReader.getNext(lJcas.getCas());
                        XmiCasSerializer lXmiCasSerializer = new XmiCasSerializer(lJcas.getTypeSystem());
                        FileOutputStream lOutputStream = new FileOutputStream(new File(lTargetDir + File.separator + lDocCounter + ".xmi"));
                        lXmiCasSerializer.serialize(lJcas.getCas(), lOutputStream);
                        lOutputStream.flush();
                        lOutputStream.close();
                        System.out.println("Exported Documents: " + lReader.getPooledDocumentsRead() + ", Effectively exported Documents: " + lReader.getDocumentsRead()+", Docs skipped by Log: "+lReader.getDocumentsSkippedByLog());
                        lDocCounter++;
                    }
                }
                break;
            }
            case "countdocs": {
                try (WikiDragonCassandraDocumentKeyIterator lReader = new WikiDragonCassandraDocumentKeyIterator(lKeyspace, lUser, lPassword, new String[]{lHost}, lDBName, lProcessingState, lSkipZero)) {
                    while (lReader.hasNext()) {
                        lReader.next();
                        System.out.println("Fetched Documents: " + lReader.getDocumentsRead());
                    }
                }
                break;
            }
        }
    }

}
