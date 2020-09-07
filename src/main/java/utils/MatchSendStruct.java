package utils;

import Structure.EventVal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class MatchSendStruct {

    public EventVal eval;
    public CopyOnWriteArraySet<String> subSet;

    public MatchSendStruct(EventVal eval) {
        this.eval = eval;
//        this.subSet = Collections.synchronizedSet(new HashSet<String>());
        this.subSet = new CopyOnWriteArraySet<>();
    }
}