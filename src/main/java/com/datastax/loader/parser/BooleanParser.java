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

import java.text.ParseException;

// Boolean parser - handles any way that Booleans can be expressed in Java
public class BooleanParser extends AbstractParser {
    private static final String BOOLSTYLE_1_0 = "1_0";
    private static final String BOOLSTYLE_T_F = "T_F";
    private static final String BOOLSTYLE_Y_N = "Y_N";
    private static final String BOOLSTYLE_YES_NO = "YES_NO";
    private static final String BOOLSTYLE_TRUE_FALSE = "TRUE_FALSE";
    private final String boolTrue;
    private final String boolFalse;

    public BooleanParser(BoolStyle inBoolStyle) {
        if (null == inBoolStyle)
            inBoolStyle = BoolStyle.BoolStyle_TrueFalse;
        boolTrue = inBoolStyle.getTrueStr();
        boolFalse = inBoolStyle.getFalseStr();
    }

    public static BoolStyle getBoolStyle(String instr) {
        for (BoolStyle bs : BoolStyle.values()) {
            if (bs.getStyle().equalsIgnoreCase(instr)) {
                return bs;
            }
        }
        return null;
    }

    public static String getOptions() {
        String ret = "'" + BOOLSTYLE_1_0 + "'";
        ret = ret + ", '" + BOOLSTYLE_T_F + "'";
        ret = ret + ", '" + BOOLSTYLE_Y_N + "'";
        ret = ret + ", '" + BOOLSTYLE_TRUE_FALSE + "'";
        ret = ret + ", '" + BOOLSTYLE_YES_NO + "'";
        return ret;
    }

    public Boolean parse(String toparse) throws ParseException {
        if (null == toparse)
            return null;
        if (boolTrue.equalsIgnoreCase(toparse))
            return Boolean.TRUE;
        if (boolFalse.equalsIgnoreCase(toparse))
            return Boolean.FALSE;
        throw new ParseException("Boolean was not TRUE (" + boolTrue + ") or FALSE (" + boolFalse + ")", 0);
    }

    public String format(Object o) {
        Boolean v = (Boolean) o;
        if (v)
            return boolTrue;
        return boolFalse;
    }

    public enum BoolStyle {
        BoolStyle_TrueFalse("TRUE_FALSE", "TRUE", "FALSE"),
        BoolStyle_10("1_0", "1", "0"),
        BoolStyle_TF("T_F", "T", "F"),
        BoolStyle_YN("Y_N", "Y", "N"),
        BoolStyle_YesNo("YES_NO", "YES", "NO");

        private final String styleStr;
        private final String trueStr;
        private final String falseStr;

        BoolStyle(String inStyleStr, String inTrueStr, String inFalseStr) {
            styleStr = inStyleStr;
            trueStr = inTrueStr;
            falseStr = inFalseStr;
        }

        public String getStyle() {
            return styleStr;
        }

        public String getTrueStr() {
            return trueStr;
        }

        public String getFalseStr() {
            return falseStr;
        }
    }
}

