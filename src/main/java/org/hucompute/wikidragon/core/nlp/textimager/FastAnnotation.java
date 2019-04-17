package org.hucompute.wikidragon.core.nlp.textimager;

import org.jetbrains.annotations.NotNull;

import java.util.TreeMap;

public class FastAnnotation implements Comparable<FastAnnotation> {

    protected FastDocument document;
    protected String typeUri;
    protected String name;
    protected int begin;
    protected int end;
    protected TreeMap<String, String> attributes;

    protected FastAnnotation(FastDocument document, String typeUri, String name, int begin, int end) {
        this.document = document;
        this.typeUri = typeUri;
        this.name = name;
        this.begin = begin;
        this.end = end;
        this.attributes = new TreeMap<>();
    }

    public FastDocument getDocument() {
        return document;
    }

    public int getBegin() {
        return begin;
    }

    public int getEnd() {
        return end;
    }

    public TreeMap<String, String> getAttributes() {
        return new TreeMap<>(attributes);
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getAttributeValue(String pKey, String pDefaultValue) {
        return attributes.getOrDefault(pKey, pDefaultValue);
    }

    public void setAttributeValue(String pKey, String pValue) {
        attributes.put(pKey, pValue);
    }

    public void remove() {
        document.annotations.remove(this);
    }

    public String getTypeUri() {
        return typeUri;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return document.getText().substring(begin, end);
    }

    @Override
    public int compareTo(@NotNull FastAnnotation o) {
        int lResult = Integer.compare(begin, o.begin);
        if (lResult == 0) {
            lResult = Integer.compare(end, o.end);
        }
        return lResult;
    }
}
