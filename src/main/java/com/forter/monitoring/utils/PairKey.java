package com.forter.monitoring.utils;

import com.google.common.base.Preconditions;

public class PairKey {
    private Object obj1;
    private Object obj2;

    public Object getObj1() {
        return obj1;
    }

    public Object getObj2() {
        return obj2;
    }

    public PairKey(Object obj1, Object obj2) {
        Preconditions.checkNotNull(obj1, "obj1 is null");
        Preconditions.checkNotNull(obj2, "obj2 is null");
        this.obj1 = obj1;
        this.obj2 = obj2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairKey pairKey = (PairKey) o;

        return (obj1 == pairKey.obj1 && obj2==pairKey.obj2);
    }

    @Override
    public int hashCode() {
        return 31 * obj1.hashCode() + obj2.hashCode();
    }


}
