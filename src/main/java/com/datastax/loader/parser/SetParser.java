/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader.parser;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class SetParser extends AbstractParser {
    private static final char collectionQuote = '\"';
    private static final char collectionEscape = '\\';
    private final Parser parser;
    private final char collectionDelim;
    private final char collectionBegin;
    private final char collectionEnd;
    private final Set<Object> elements;

    private CsvParser csvp = null;

    public SetParser(Parser inParser, char inCollectionDelim,
                     char inCollectionBegin, char inCollectionEnd) {
        parser = inParser;
        collectionDelim = inCollectionDelim;
        collectionBegin = inCollectionBegin;
        collectionEnd = inCollectionEnd;
        elements = new HashSet<>();

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.getFormat().setDelimiter(collectionDelim);
        settings.getFormat().setQuote(collectionQuote);
        settings.getFormat().setQuoteEscape(collectionEscape);

        csvp = new CsvParser(settings);
    }

    public Object parse(String toparse) throws ParseException {
        if (null == toparse)
            return null;
        toparse = unquote(toparse);
        if (!toparse.startsWith(Character.toString(collectionBegin)))
            throw new ParseException("Must begin with " + collectionBegin
                    + "\n", 0);
        if (!toparse.endsWith(Character.toString(collectionEnd)))
            throw new ParseException("Must end with " + collectionEnd
                    + "\n", 0);
        toparse = toparse.substring(1, toparse.length() - 1);
        String[] row = csvp.parseLine(toparse);
        elements.clear();
        try {
            for (String aRow : row) {
                if (null == aRow)
                    throw new ParseException("Sets may not have NULLs\n", 0);
                Object o = parser.parse(aRow);
                if (null == o)
                    throw new ParseException("Sets may not have NULLs\n", 0);

                elements.add(o);
            }
        } catch (Exception e) {
            throw new ParseException("Trouble parsing : " + e.getMessage(), 0);
        }
        return elements;
    }

    //public String format(Row row, int index) {
    //  if (row.isNull(index))
    //      return null;
    //  Set<Object> set = row.getSet(index, Object.class);
    @SuppressWarnings("unchecked")
    public String format(Object o) {
        Set<Object> set = (Set<Object>) o;
        Iterator<Object> iter = set.iterator();
        StringBuilder sb = new StringBuilder();
        sb.append(collectionBegin);
        if (iter.hasNext())
            sb.append(parser.format(iter.next()));
        while (iter.hasNext()) {
            sb.append(collectionDelim);
            sb.append(parser.format(iter.next()));
        }
        sb.append(collectionEnd);

        return quote(sb.toString());
    }
}
