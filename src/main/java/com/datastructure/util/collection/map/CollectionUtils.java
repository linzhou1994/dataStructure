package com.datastructure.util.collection.map;

import java.util.Collection;
import java.util.List;

public class CollectionUtils {
    public static  boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    public static  boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }


}
