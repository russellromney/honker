package dev.honker;

import java.util.ArrayList;
import java.util.List;

final class Params {
    private Params() {
    }

    static List<Object> of(Object... values) {
        ArrayList<Object> out = new ArrayList<>(values.length);
        for (Object value : values) {
            out.add(value);
        }
        return out;
    }
}
