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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class DelimParser {
    private static final String DEFAULT_DELIMITER = ",";
    private static final String DEFAULT_NULLSTRING = "";
    private static final char DEFAULT_QUOTE = '\"';
    private static final char DEFAULT_ESCAPE = '\\';
    private final List<Parser> parsers;
    private int parsersSize;
    private final List<Object> elements;
    private final String delimiter;
    private final String nullString;
    private final List<Boolean> skip;
    private CsvParser csvp = null;

    public DelimParser(String inDelimiter, String inNullString) {
        this(inDelimiter, inNullString, DEFAULT_QUOTE, DEFAULT_ESCAPE, null);
    }

    public DelimParser(String inDelimiter, String inNullString, Character inQuote, Character inEscape, Integer inMaxCharsPerColumn) {
        parsers = new ArrayList<>();
        elements = new ArrayList<>();
        skip = new ArrayList<>();
        parsersSize = parsers.size();
        if (null == inDelimiter) {
            delimiter = DEFAULT_DELIMITER;
        } else {
            delimiter = inDelimiter;
        }
        if (null == inNullString) {
            nullString = DEFAULT_NULLSTRING;
        } else {
            nullString = inNullString;
        }
        char delim = ("\\t".equals(delimiter)) ? '\t' : delimiter.charAt(0);

        char quote;
        if (null == inQuote) {
            quote = DEFAULT_QUOTE;
        } else {
            quote = inQuote;
        }

        char escape;
        if (null == inEscape) {
            escape = DEFAULT_ESCAPE;
        } else {
            escape = inEscape;
        }

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.getFormat().setDelimiter(delim);
        settings.getFormat().setQuote(quote);
        settings.getFormat().setQuoteEscape(escape);
        if (inMaxCharsPerColumn != null) {
            settings.setMaxCharsPerColumn(inMaxCharsPerColumn);
        }
        csvp = new CsvParser(settings);
    }

    // Adds a parser to the list
    public void add(Parser p) {
        parsers.add(p);
        skip.add(false);
        parsersSize = parsers.size();
    }

    public void addSkip(int idx) {
        parsers.add(idx, new StringParser());
        skip.add(idx, true);
        parsersSize = parsers.size();
    }

    public List<Object> parse(String line) {
        //return parseComplex(line);
        return parseWithUnivocity(line);
    }

    public List<Object> parseWithUnivocity(String line) {
        String[] row = csvp.parseLine(line);
        if (row.length != parsersSize) {
            System.err.println("Row has different number of fields (" + row.length + ") than expected (" + parsersSize + ")");
            return null;
        }
        elements.clear();
        Object toAdd;
        for (int i = 0; i < parsersSize; i++) {
            try {
                if ((null == row[i]) ||
                        ((null != nullString) &&
                                (nullString.equalsIgnoreCase(row[i]))))
                    toAdd = null;
                else
                    toAdd = parsers.get(i).parse(row[i]);

                if (!skip.get(i))
                    elements.add(toAdd);
            } catch (NumberFormatException e) {
                System.err.println(String.format("Invalid number in input number %d: %s", i, e.getMessage()));
                return null;
            } catch (ParseException pe) {
                System.err.println(String.format("Invalid format in input %d: %s", i, pe.getMessage()));
                return null;
            }
        }

        return elements;
    }

    public String format(Row row) throws IndexOutOfBoundsException, InvalidTypeException {
        String s;
        StringBuilder retVal = new StringBuilder();
        s = parsers.get(0).format(row, 0);
        if (null == s)
            s = nullString;
        retVal.append(s);
        for (int i = 1; i < parsersSize; i++) {
            s = parsers.get(i).format(row, i);
            if (null == s)
                s = nullString;
            retVal.append(delimiter).append(s);
        }
        return retVal.toString();
    }
}
