/*
 * Suffix Tree implementation
 * Copyright 2021 André Vicentini (https://github.com/av1ctor/)
 * Licensed under the Apache license Version 2.0
 * Ported from https://github.com/sykesd/suffixtree (Copyright 2012 Alessandro Bahgat Shehata, Licensed under the Apache license Version 2.0) 
 */

import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";
import Char "mo:base/Char";
import Text "mo:base/Text";
import Buffer "mo:base/Buffer";
import Array "mo:base/Array";
import HashMap "mo:base/HashMap";
import Set "mo:base/TrieSet";
import Hash "mo:base/Hash";
import Iter "mo:base/Iter";
import Option "mo:base/Option";
import P "mo:base/Prelude";
import D "mo:base/Debug";
import Utils "./utils";
import CharUtils "./charutils";

module {
    class Edge<E>(
        lbl: [Char], 
        dst: Node<E>
    ) {
        public var label_: [Char] = lbl;
        public let dest = dst;
    };
    
    class Node<E>(
        id_: Nat32
    ) {
        public let id = id_;
        public let edges = HashMap.HashMap<Char, Edge<E>>(8, Char.equal, Char.toNat32);
        public var data = Set.empty<E>();
        public var suffix: ?Node<E> = null;

        public func equal(other: Node<E>): Bool = id == other.id;

        public func addRef(
            value: E,
            hash: (E) -> Hash.Hash,
            equal: (E, E) -> Bool
        ) {
            if(Set.mem<E>(data, value, hash(value), equal)) {
                return;
            };
            
            data := Set.put<E>(data, value, hash(value), equal);

            var s = suffix;
            label l loop {
                switch(s) {
                    case null {
                        break l;
                    };
                    case (?suffix) {
                        suffix.addRef(value, hash, equal);
                        s := suffix.suffix;
                    };
                };
            };
        };
    };
    
    public class SuffixTree<E>(
        hash: (E) -> Hash.Hash,
        equal: (E, E) -> Bool
    ) {
        var _id: Nat32 = Nat32.fromNat(0);
        func nextId(): Nat32 {
            // NOTE: assuming this class will only be used by an actor
            _id +%= 1;
            _id;
        };

        public let root: Node<E> = Node<E>(nextId());
        var activeLeaf = root;

        ///
        public func find(
            pattern: Text
        ): ?Set.Set<E> {
            switch(findNode(CharUtils.fromText(pattern))) {
                case null {
                    null;
                };
                case (?node) {
                    ?collectData(node);
                };
            };
        };

        ///
        public func delete(
            key: Text,
            value: E
        ) {
            switch(findNode(CharUtils.fromText(key))) {
                case null {
                };
                case (?node) {
                    removeFromData(node, value);
                };
            };
        };

        ///
        public func put(
            key: Text,
            value: E
        ) {

            activeLeaf := root;

            let remainder = CharUtils.fromText(key);
            let size = remainder.size();

            var node = root;
            var text = Buffer.Buffer<Char>(size);

            var i = 0;
            while (i < size) {
                let char = remainder[i];
                
                text.add(char);

                let rem = CharUtils.substring(remainder, i, size - 1);
                let updated = update(node, Buffer.toArray(text), rem, value);

                let canonized = canonize(updated.0, updated.1);
                node := canonized.0;
                text := CharUtils.toBuffer(canonized.1);
                
                i += 1;
            };

            if (Option.isNull(activeLeaf.suffix) and 
                not activeLeaf.equal(root) and 
                not activeLeaf.equal(node)) {
                activeLeaf.suffix := ?node;
            };
        };

        func collectData(
            node: Node<E>
        ): Set.Set<E> {
            var res = Set.union<E>(Set.empty<E>(), node.data, equal);
            
            for((_, edge) in node.edges.entries()) {
                res := Set.union<E>(res, collectData(edge.dest), equal);
            };

            res;
        };

        func removeFromData(
            node: Node<E>,
            value: E
        ) {
            node.data := Set.delete<E>(node.data, value, hash(value), equal);
            
            for((_, edge) in node.edges.entries()) {
                removeFromData(edge.dest, value);
            };

            if(Set.size<E>(node.data) == 0) {
                for((edge, _) in node.edges.entries()) {
                    node.edges.delete(edge);
                };
            };
        };

        func findNode(
            pattern: [Char]
        ): ?Node<E> {

            var node = root;

            var i = 0;
            while (i < pattern.size()) {
                let char = pattern[i];
                let edge = node.edges.get(char);
                switch(edge) {
                    case null {
                        return null;
                    };
                    case (?edge) {
                        let label_ = edge.label_;
                        let len = Nat.min(pattern.size() - i, label_.size());
                        if (not CharUtils.regionMatches(pattern, i, label_, len)) {
                            return null;
                        };

                        if (Nat.greaterOrEqual(label_.size(), pattern.size() - i)) {
                            return ?edge.dest;
                        };

                        node := edge.dest;
                        i += len;
                    };
                };
            };

            return null;
        };

        func testAndSplit(
            inputs: Node<E>,
            part: [Char],
            char: Char,
            remainder: [Char],
            value: E
        ): (Bool, Node<E>) {

            let (node, text) = canonize(inputs, part);

            if (text.size() > 0) {
                switch(node.edges.get(text[0])) {
                    case null {
                        assert false; loop { };
                        return (false, node);
                    };
                    case (?g) {
                        let label_ = g.label_; 

                        if (label_.size() > text.size() and label_[text.size()] == char) {
                            return (true, node);
                        } 
                        else {
                            let newLabel = CharUtils.substring(label_, text.size(), label_.size() - 1);
                            g.label_ := newLabel;

                            let r = Node<E>(nextId());
                            let newEdge = Edge<E>(text, r);

                            r.edges.put(newLabel[0], g);
                            node.edges.put(text[0], newEdge);

                            return (false, r);
                        };
                    };
                };
            } 
            else {
                switch(node.edges.get(char)) {
                    case null {
                        return (false, node);
                    };
                    case (?e) {
                        let label_ = e.label_;
                        if(CharUtils.equals(remainder, label_)) {
                            e.dest.addRef(value, hash, equal);
                            return (true, node);
                        } 
                        else if (CharUtils.startsWith(remainder, label_)) {
                            return (true, node);
                        } 
                        else if (CharUtils.startsWith(label_, remainder)) {
                            let newNode = Node<E>(nextId());
                            newNode.addRef(value, hash, equal);

                            let newEdge = Edge<E>(remainder, newNode);

                            let newLabel = CharUtils.substring(label_, remainder.size(), label_.size() - 1);
                            e.label_ := newLabel;

                            newNode.edges.put(newLabel[0], e);
                            node.edges.put(char, newEdge);

                            return (false, node);
                        } 
                        else {
                            return (true, node);
                        };
                    };
                };
            };
        };

        func canonize(
            s: Node<E>, 
            inputstr: [Char]
        ): (Node<E>, [Char]) {

            if(inputstr.size() == 0) {
                return (s, inputstr);
            };
            
            var node = s;
            var str = inputstr;
            var g = s.edges.get(str[0]);
            label can_loop while (Option.isSome(g)) {
                switch(g) {
                    case null {
                        assert false; loop { };
                    };
                    case (?edge) {
                        if(not CharUtils.startsWith(str, edge.label_)) {
                            break can_loop;
                        };
                        str := CharUtils.substring(str, edge.label_.size(), str.size() - 1);
                        node := edge.dest;
                        if(str.size() > 0) {
                            g := node.edges.get(str[0]);
                        };
                    };
                };
            };

            (node, str);
        };

        func update(
            inputNode: Node<E>, 
            stringPart: [Char], 
            rest: [Char], 
            value: E
        ): (Node<E>, [Char]) {
            
            var node = inputNode;
            var tempstr = stringPart;
            let char = tempstr[stringPart.size()-1];

            var oldroot = root;

            let split = testAndSplit(node, CharUtils.removeLastChar(tempstr), char, rest, value);
            var endpoint = split.0;
            var r = split.1;

            while (not endpoint) {
                let leaf = switch(r.edges.get(char)) {
                    case (?edge) {
                        edge.dest;
                    };
                    case null {
                        let l = Node<E>(nextId());
                        l.addRef(value, hash, equal);
                        let e = Edge<E>(rest, l);
                        r.edges.put(char, e);
                        l;
                    };
                };

                if (not activeLeaf.equal(root)) {
                    activeLeaf.suffix := ?leaf;
                };
                activeLeaf := leaf;

                if (not oldroot.equal(root)) {
                    oldroot.suffix := ?r;
                };

                oldroot := r;

                switch(node.suffix) {
                    case null {
                        tempstr := CharUtils.removeFirstChar(tempstr);
                    }; 
                    case (?suffix) {
                        let canonized = canonize(suffix, CharUtils.removeLastChar(tempstr));
                        node := canonized.0;
                        tempstr := CharUtils.appendChar(canonized.1, CharUtils.getLastChar(tempstr));
                    };
                };

                let split = testAndSplit(node, CharUtils.removeLastChar(tempstr), char, rest, value);
                endpoint := split.0;
                r := split.1;
            };

            if (not oldroot.equal(root)) {
                oldroot.suffix := ?r;
            };

            (node, tempstr);
        };        
    };
};