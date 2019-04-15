package org.hucompute.wikidragon.nlp.textimager;

import org.json.JSONObject;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.DefaultHandler2;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;

public class FastDocument {

    public static final String NS_SEGMENTATION_URI = "http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore";
    public static final String NS_WIKIDRAGON_URI = "http:///org/hucompute/wikidragon/core/nlp/annotation.ecore";
    private static final String NS_CAS_URI = "http:///uima/cas.ecore";
    private static final String NS_XMI_URI = "http://www.omg.org/XMI";
    private static final String NS_TCAS_URI = "http:///uima/tcas.ecore";
    private static final String NS_XMI_PREFIX = "xmi";
    private static final String NS_CAS_PREFIX = "cas";
    private static final String NS_TCAS_PREFIX = "tcas";
    public static final String NS_WIKIDRAGON_PREFIX = "wikidragon";
    public static final String NS_SEGMENTATION_PREFIX = "seg";

    private static Map<String, String> defaultUriPrefixMap;

    protected String text;
    protected Set<String> typeUris;
    protected List<FastAnnotation> annotations;
    protected String language;

    static {
        defaultUriPrefixMap = new HashMap<>();
        defaultUriPrefixMap.put(NS_CAS_URI, NS_CAS_PREFIX);
        defaultUriPrefixMap.put(NS_TCAS_URI, NS_TCAS_PREFIX);
        defaultUriPrefixMap.put(NS_XMI_URI, NS_XMI_PREFIX);
        defaultUriPrefixMap.put(NS_WIKIDRAGON_URI, NS_WIKIDRAGON_PREFIX);
        defaultUriPrefixMap.put(NS_SEGMENTATION_URI, NS_SEGMENTATION_PREFIX);
    }

    public FastDocument(String pLanguage) {
        this(pLanguage, "");
    }

    public FastDocument(String pLanguage, String pSofa) {
        text = pSofa;
        typeUris = new HashSet<>();
        annotations = new ArrayList<>();
        language = pLanguage;
    }

    public FastAnnotation addAnnotation(String pTypeUri, String pName, int pBegin, int pEnd) {
        typeUris.add(pTypeUri);
        FastAnnotation lAnnotation = new FastAnnotation(this, pTypeUri, pName, pBegin, pEnd);
        annotations.add(lAnnotation);
        return lAnnotation;
    }

    public String getSubString(int pBegin, int pEnd) {
        return text.substring(pBegin, pEnd);
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getText() {
        return text;
    }

    public List<FastAnnotation> getAnnotations(boolean pSorted) {
        List<FastAnnotation> lResult = new ArrayList<>(annotations);
        if (pSorted) Collections.sort(lResult);
        return lResult;
    }

    public List<FastAnnotation> getAnnotations(String pTypeUri, String pName, boolean pSorted) {
        List<FastAnnotation> lResult = new ArrayList<>();
        for (FastAnnotation lAnnotation:annotations) {
            if (lAnnotation.getTypeUri().equals(pTypeUri) && lAnnotation.getName().equals(pName)) lResult.add(lAnnotation);
        }
        if (pSorted) Collections.sort(lResult);
        return lResult;
    }

    public List<FastAnnotation> getSubsumedAnnotations(FastAnnotation pParent, boolean pSorted) {
        List<FastAnnotation> lResult = new ArrayList<>();
        for (FastAnnotation lAnnotation:annotations) {
            if ((lAnnotation.getBegin() >= pParent.getBegin()) && (lAnnotation.getEnd() <= pParent.getEnd())) {
                lResult.add(lAnnotation);
            }
        }
        if (pSorted) Collections.sort(lResult);
        return lResult;
    }

    public List<FastAnnotation> getSubsumedAnnotations(int pBegin, int pEnd, boolean pSorted) {
        List<FastAnnotation> lResult = new ArrayList<>();
        for (FastAnnotation lAnnotation:annotations) {
            if ((lAnnotation.getBegin() >= pBegin) && (lAnnotation.getEnd() <= pEnd)) {
                lResult.add(lAnnotation);
            }
        }
        if (pSorted) Collections.sort(lResult);
        return lResult;
    }

    public String getLanguage() {
        return language;
    }

    public Set<String> getTypeUris() {
        return new HashSet<>(typeUris);
    }

    public String exportXMI() throws IOException {
        return exportXMI(false);
    }

    public String exportXMI(boolean pExportAnnotationLabels) throws IOException {
        // Collect Types
        Map<String, String> lUriPrefixMap = new HashMap<>(defaultUriPrefixMap);
        int lCounter = 0;
        for (String lTypeUri:typeUris) {
            if (!lUriPrefixMap.containsKey(lTypeUri)) {
                lCounter++;
                lUriPrefixMap.put(lTypeUri, "type"+lCounter);
            }
        }
        try {
            StringWriter lStringWriter = new StringWriter();
            XMLOutputFactory lXMLOutputFactory = XMLOutputFactory.newInstance();
            XMLStreamWriter lXMLStreamWriter = lXMLOutputFactory.createXMLStreamWriter(lStringWriter);
            String lSofaID = "1";
            int lIDCounter = 1;
            lXMLStreamWriter.writeStartDocument("utf-8", "1.0");
            // Root Element
            lXMLStreamWriter.writeStartElement(lUriPrefixMap.get(NS_XMI_URI), "XMI", NS_XMI_URI);
            lXMLStreamWriter.writeNamespace(lUriPrefixMap.get(NS_XMI_URI), NS_XMI_URI);
            for (Map.Entry<String, String> lTypeEntry:lUriPrefixMap.entrySet()) {
                if (!lTypeEntry.getKey().equals(NS_XMI_URI)) {
                    lXMLStreamWriter.writeNamespace(lTypeEntry.getValue(), lTypeEntry.getKey());
                }
            }
            lXMLStreamWriter.writeAttribute(lUriPrefixMap.get(NS_XMI_URI), NS_XMI_URI, "version", "2.0");
            // Null Element
            lXMLStreamWriter.writeStartElement(lUriPrefixMap.get(NS_CAS_URI), "NULL", NS_CAS_URI);
            lXMLStreamWriter.writeAttribute(lUriPrefixMap.get(NS_XMI_URI), NS_XMI_URI, "id", "0");
            lXMLStreamWriter.writeEndElement();
            // Write DocumentAnnotation
            lXMLStreamWriter.writeStartElement(lUriPrefixMap.get(NS_TCAS_URI), "DocumentAnnotation", NS_TCAS_URI);
            lXMLStreamWriter.writeAttribute(lUriPrefixMap.get(NS_XMI_URI), NS_XMI_URI, "id", Integer.toString(++lIDCounter));
            StringBuilder lMembers = new StringBuilder(Integer.toString(lIDCounter));
            lXMLStreamWriter.writeAttribute("sofa", lSofaID);
            lXMLStreamWriter.writeAttribute("begin", "0");
            lXMLStreamWriter.writeAttribute("end", Integer.toString(text.length()));
            lXMLStreamWriter.writeAttribute("language", language);
            lXMLStreamWriter.writeEndElement();
            // Write Annotations
            List<FastAnnotation> lAnnotations = new ArrayList<>(annotations);
            Collections.sort(lAnnotations);
            for (FastAnnotation lAnnotation:lAnnotations) {
                lXMLStreamWriter.writeStartElement(lUriPrefixMap.get(lAnnotation.getTypeUri()), lAnnotation.getName(), lAnnotation.getTypeUri());
                lXMLStreamWriter.writeAttribute(lUriPrefixMap.get(NS_XMI_URI), NS_XMI_URI, "id", Integer.toString(++lIDCounter));
                lXMLStreamWriter.writeAttribute("sofa", lSofaID);
                lXMLStreamWriter.writeAttribute("begin", Integer.toString(lAnnotation.getBegin()));
                lXMLStreamWriter.writeAttribute("end", Integer.toString(lAnnotation.getEnd()));
                for (Map.Entry<String, String> lEntry:lAnnotation.getAttributes().entrySet()) {
                    lXMLStreamWriter.writeAttribute(lEntry.getKey(), lEntry.getValue());
                }
                lMembers.append(" "+lIDCounter);
                if (pExportAnnotationLabels) {
                    lXMLStreamWriter.writeCharacters(lAnnotation.toString().replace((char)0, ' '));
                }
                lXMLStreamWriter.writeEndElement();
            }
            // Write Sofa
            lXMLStreamWriter.writeStartElement(lUriPrefixMap.get(NS_CAS_URI), "Sofa", NS_CAS_URI);
            lXMLStreamWriter.writeAttribute(lUriPrefixMap.get(NS_XMI_URI), NS_XMI_URI, "id", lSofaID);
            lXMLStreamWriter.writeAttribute("sofaNum", "1");
            lXMLStreamWriter.writeAttribute("sofaID", "_InitialView");
            lXMLStreamWriter.writeAttribute("mimeType", "text");
            lXMLStreamWriter.writeAttribute("sofaString", replaceInvalidControlCharactersXML10WithWS(text));

            lXMLStreamWriter.writeEndElement();
            // Write View
            lXMLStreamWriter.writeStartElement(lUriPrefixMap.get(NS_CAS_URI), "View", NS_CAS_URI);
            lXMLStreamWriter.writeAttribute("sofa", lSofaID);
            lXMLStreamWriter.writeAttribute("members", lMembers.toString());
            lXMLStreamWriter.writeEndElement();
            // Close Root Element
            lXMLStreamWriter.writeEndElement();
            lXMLStreamWriter.writeEndDocument();
            lXMLStreamWriter.flush();
            return lStringWriter.toString();
        }
        catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    public static FastDocument fromXMI(String pXMI) throws IOException {
        try {
            SAXParserFactory lSAXParserFactory = SAXParserFactory.newInstance();
            lSAXParserFactory.setValidating(false);
            lSAXParserFactory.setNamespaceAware(true);
            SAXParser lParser = lSAXParserFactory.newSAXParser();
            Map<String, String> lPrefixUriMap = new HashMap<>();
            Map<String, String> lUriPrefixMap = new HashMap<>();
            FastDocument lFastDocument = new FastDocument("");
            DefaultHandler2 lHandler = new DefaultHandler2() {

                @Override
                public void startDocument() throws SAXException {
                    super.startDocument();
                }

                @Override
                public void endDocument() throws SAXException {
                    super.endDocument();
                }


                @Override
                public void startPrefixMapping(String prefix, String uri) throws SAXException {
                    lPrefixUriMap.put(prefix, uri);
                    lUriPrefixMap.put(uri, prefix);
                }

                @Override
                public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                    switch (uri) {
                        case NS_TCAS_URI: {
                            if (localName.equals("DocumentAnnotation")) {
                                lFastDocument.setLanguage(attributes.getValue("language"));
                            }
                            break;
                        }
                        case NS_CAS_URI: {
                            if (localName.equals("Sofa")) {
                                lFastDocument.setText(attributes.getValue("sofaString"));
                            }
                            break;
                        }
                        default: {
                            Map<String, String> lAttributeMap = new HashMap<>();
                            for (int i=0; i<attributes.getLength(); i++) {
                                lAttributeMap.put(attributes.getQName(i), attributes.getValue(i));
                            }
                            String lSofa = lAttributeMap.get("sofa");
                            String lBegin = lAttributeMap.get("begin");
                            String lEnd = lAttributeMap.get("end");
                            if ((lSofa != null) && (lBegin != null) && (lEnd != null)) {
                                FastAnnotation lAnnotation = lFastDocument.addAnnotation(uri, localName, Integer.parseInt(lBegin), Integer.parseInt(lEnd));
                                for (Map.Entry<String, String> lEntry:lAttributeMap.entrySet()) {
                                    switch (lEntry.getKey()) {
                                        case "sofa":
                                        case "begin":
                                        case "end":
                                        case "xmi:id": {
                                            break;
                                        }
                                        default: {
                                            lAnnotation.setAttributeValue(lEntry.getKey(), lEntry.getValue());
                                        }
                                    }
                                }
                            }
                            break;
                        }
                    }
                }

                @Override
                public void endElement(String uri, String localName, String qName) throws SAXException {
                }

                @Override
                public void characters(char[] ch, int start, int length) throws SAXException {
                }
            };
            lParser.parse(new InputSource(new StringReader(pXMI)), lHandler);
            return lFastDocument;
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
        catch (SAXException e) {
            throw new IOException(e);
        }
    }

    public static String replaceInvalidControlCharactersXML10WithWS(String pString) {
        StringBuilder lResult = new StringBuilder();
        PrimitiveIterator.OfInt i = pString.codePoints().iterator();
        while (i.hasNext()) {
            int lCodePoint = i.nextInt();
            if (lCodePoint<32) {
                switch (lCodePoint) {
                    case 9:
                    case 10:
                    case 13: {
                        lResult.appendCodePoint(lCodePoint);
                        break;
                    }
                    default: {
                        lResult.appendCodePoint(' ');
                        break;
                    }
                }
            }
            else {
                lResult.appendCodePoint(lCodePoint);
            }
        }
        return lResult.toString();
    }

}
