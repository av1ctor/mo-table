/**
 * B+tree implementation
 * Copyright 2021 André Vicentini (https://github.com/av1ctor/)
 * Licensed under the Apache license Version 2.0
 */

import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";
import Iter "mo:base/Iter";
import Option "mo:base/Option";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import D "mo:base/Debug";

module {
    public class Element<K, V>(
        key_: K,
        value_: ?V,
        owner_: Node<K, V>
    ) {
        public var key: K = key_;
        public var value: ?V = value_;
        public var owner: Node<K, V> = owner_;
        public var left: ?Node<K, V> = null;
        public var prev: ?Element<K, V> = null;
        public var next: ?Element<K, V> = null;

        public func clone(
        ): Element<K, V> {
            let e = Element(key, value, owner);
            e.left := left;
            return e;
        };
    };

    class ElementList<K, V>() {
        public var head: ?Element<K, V> = null;
        public var tail: ?Element<K, V> = null;
        public var cnt: Nat32 = Nat32.fromNat(0);

        public func add(
            elm: Element<K, V>,
            next: ?Element<K, V>
        ) {
            elm.next := next;

            switch(head) {
                case null {
                    elm.prev := null;
                    head := ?elm;
                    tail := ?elm;
                };
                case (?h) {
                    switch(next) {
                        case null {
                            elm.prev := tail;
                            switch(tail) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?t) {
                                    t.next := ?elm;
                                };
                            };
                            tail := ?elm;
                        };
                        case (?next) {
                            elm.prev := next.prev;
                            switch(next.prev) {
                                case null {
                                    head := ?elm;
                                };
                                case (?prev) {
                                    prev.next := ?elm;
                                };
                            };
                            next.prev := ?elm;
                        };
                    };
                };
            };

            cnt := Nat32.add(cnt, 1);
        };

        public func del(
            elm: Element<K, V>
        ) {
            switch(elm.prev) {
                case null {
                    head := elm.next;
                };
                case (?prev) {
                    prev.next := elm.next;
                };
            };

            switch(elm.next) {
                case null {
                    tail := elm.prev;
                };
                case (?next) {
                    next.prev := elm.prev;
                };
            };

            cnt := Nat32.sub(cnt, 1);
        };
    };

    public class Node<K, V>(
        id_: Nat32,
        isLeaf_: Bool,
        parent_: ?Node<K, V>
    ) {
        public let id: Nat32 = id_;
        public var isLeaf: Bool = isLeaf_;
        // nodes
        public var parent: ?Node<K, V> = parent_;
        public var prev: ?Node<K, V> = null;
        public var next: ?Node<K, V> = null;
        public var last: ?Node<K, V> = null;
        // elements
        public var elements: ElementList<K, V> = ElementList<K, V>();
    };
    
    public class BPlusTree<K, V>(
        order_: Nat,
        cmp: (K, K) -> Int
    ) {
        let order: Nat32 = Nat32.fromNat(order_);
        let minElements: Nat32 = ((order >> 1) + (order & 1)) - 1;
        let maxElements: Nat32 = order - 1;

        var id: Nat32 = Nat32.fromNat(0);
        func _nextId(): Nat32 {
            // NOTE: assuming this class will only be used by an actor
            id +%= 1;
            return id;
        };

        // nodes
        var root: Node<K, V> = Node<K, V>(_nextId(), true, null);
        var head = root;
        var tail = root;

        func _findLeaf(
            key: K
        ): Node<K, V> {
            var n = root;
            var elm = n.elements.head;
            label l while(not n.isLeaf) {
                switch(elm) {
                    case null {
                        break l;
                    };
                    case (?e) {
                        if(cmp(key, e.key) < 0) {
                            n := switch(e.left) {
                                case null {
                                    assert false; loop { };
                                }; 
                                case (?left) {
                                    left;
                                };
                            };
                            elm := n.elements.head;
                        }
                        else {
                            switch(e.next) {
                                case null {
                                    n := switch(n.last) {
                                        case null {
                                            assert false; loop { };
                                        }; 
                                        case (?last) {
                                            last;
                                        };
                                    };
                                    elm := n.elements.head;
                                };
                                case (?next) {
                                    elm := ?next;
                                };
                            };
                        };
                    };
                };
            };

            return n;
        };
        
        func _findElement(
            node: Node<K, V>,
            key: K
        ): ?Element<K, V> {
            var e = node.elements.head;
            
            label l loop {
                switch(e) {
                    case null {
                        break l;
                    };
                    case (?elm) {
                        if(cmp(elm.key, key) == 0) {
                            break l;
                        };

                        e := elm.next;
                    };
                };
            };

            return e;
        };

        func _findElementsNeq(
            node: Node<K, V>,
            key: K
        ): [Element<K, V>] {
            let res = Buffer.Buffer<Element<K, V>>(10);

            var e = node.elements.head;
            label l loop {
                switch(e) {
                    case null {
                        break l;
                    };
                    case (?elm) {
                        if(cmp(elm.key, key) != 0) {
                            res.add(elm);
                        };

                        e := elm.next;
                    };
                };
            };

            return res.toArray();
        };

        func _findElementsLt(
            node: Node<K, V>,
            key: K
        ): [Element<K, V>] {
            let res = Buffer.Buffer<Element<K, V>>(10);

            var e = node.elements.head;
            label l loop {
                switch(e) {
                    case null {
                        break l;
                    };
                    case (?elm) {
                        if(cmp(elm.key, key) < 0) {
                            res.add(elm);
                        };

                        e := elm.next;
                    };
                };
            };

            return res.toArray();
        };

        func _findElementsLte(
            node: Node<K, V>,
            key: K
        ): [Element<K, V>] {
            let res = Buffer.Buffer<Element<K, V>>(10);

            var e = node.elements.head;
            label l loop {
                switch(e) {
                    case null {
                        break l;
                    };
                    case (?elm) {
                        if(cmp(elm.key, key) <= 0) {
                            res.add(elm);
                        };

                        e := elm.next;
                    };
                };
            };

            return res.toArray();
        };

        func _findElementsGt(
            node: Node<K, V>,
            key: K
        ): [Element<K, V>] {
            let res = Buffer.Buffer<Element<K, V>>(10);

            var e = node.elements.head;
            label l loop {
                switch(e) {
                    case null {
                        break l;
                    };
                    case (?elm) {
                        if(cmp(elm.key, key) > 0) {
                            res.add(elm);
                        };

                        e := elm.next;
                    };
                };
            };

            return res.toArray();
        };

        func _findElementsGte(
            node: Node<K, V>,
            key: K
        ): [Element<K, V>] {
            let res = Buffer.Buffer<Element<K, V>>(10);

            var e = node.elements.head;
            label l loop {
                switch(e) {
                    case null {
                        break l;
                    };
                    case (?elm) {
                        if(cmp(elm.key, key) >= 0) {
                            res.add(elm);
                        };

                        e := elm.next;
                    };
                };
            };

            return res.toArray();
        };

        func _findElementsBetween(
            node: Node<K, V>,
            a: K,
            b: K
        ): [Element<K, V>] {
            let res = Buffer.Buffer<Element<K, V>>(10);

            var e = node.elements.head;
            label l loop {
                switch(e) {
                    case null {
                        break l;
                    };
                    case (?elm) {
                        if(cmp(elm.key, a) >= 0 and cmp(elm.key, b) <= 0) {
                            res.add(elm);
                        };

                        e := elm.next;
                    };
                };
            };

            return res.toArray();
        };

        ///
        public func get(
            key: K
        ): ?V {
            if(Option.isNull(root.elements.head)) {
                return null;
            };

            let l = _findLeaf(key);
            let e = _findElement(l, key);

            switch(e) {
                case null {
                    return null;
                };
                case (?e) {
                    return e.value;
                }
            };
        };

        ///
        public func findNeq(
            key: K
        ): [?V] {
            if(Option.isNull(root.elements.head)) {
                return [];
            };

            let l = _findLeaf(key);
            let elms = _findElementsNeq(l, key);

            return Array.map(elms, func(e: Element<K, V>): ?V = e.value);
        };

        ///
        public func findLt(
            key: K
        ): [?V] {
            if(Option.isNull(root.elements.head)) {
                return [];
            };

            let l = _findLeaf(key);
            let elms = _findElementsLt(l, key);

            return Array.map(elms, func(e: Element<K, V>): ?V = e.value);
        };

        ///
        public func findLte(
            key: K
        ): [?V] {
            if(Option.isNull(root.elements.head)) {
                return [];
            };

            let l = _findLeaf(key);
            let elms = _findElementsLte(l, key);

            return Array.map(elms, func(e: Element<K, V>): ?V = e.value);
        };

        ///
        public func findGt(
            key: K
        ): [?V] {
            if(Option.isNull(root.elements.head)) {
                return [];
            };

            let l = _findLeaf(key);
            let elms = _findElementsGt(l, key);

            return Array.map(elms, func(e: Element<K, V>): ?V = e.value);
        };

        ///
        public func findGte(
            key: K
        ): [?V] {
            if(Option.isNull(root.elements.head)) {
                return [];
            };

            let l = _findLeaf(key);
            let elms = _findElementsGte(l, key);

            return Array.map(elms, func(e: Element<K, V>): ?V = e.value);
        };

        ///
        public func findBetween(
            a: K,
            b: K
        ): [?V] {
            if(Option.isNull(root.elements.head)) {
                return [];
            };

            let l = _findLeaf(a);
            let elms = _findElementsBetween(l, a, b);

            return Array.map(elms, func(e: Element<K, V>): ?V = e.value);
        };

        ///
        public func getFirstLeaf(
        ): Node<K, V> {
            return head;
        };

        ///
        public func getLastLeaf(
        ): Node<K, V> {
            return tail;
        };

        func _addElement(
            key: K,
            node: Node<K, V>,
            value: ?V
        ): Element<K, V> {
            var elm = node.elements.head;

            label l loop {
                switch(elm) {
                    case null {
                        break l;
                    };
                    case (?e) {
                        if(cmp(e.key, key) >= 0) {
                            break l;
                        };
                        elm := e.next;
                    };
                };
            };

            let res = Element<K, V>(key, value, node);
            node.elements.add(res, elm);
            return res;
        };

        ///
        public func put(
            key: K, 
            value: ?V
        ) {
            if(Option.isNull(root.elements.head)) {
                ignore _addElement(key, root, value);
                return;
            };

            let l = _findLeaf(key);
            let e = _findElement(l, key);

            switch(e) {
                case (?e) {
                    e.value := value;
                };
                case null {
                    ignore _addElement(key, l, value);
                    if(Nat32.greater(l.elements.cnt, maxElements)) {
                        _splitLeaf(l);
                    };
                };
            };
        };

        func _findSplitKey(
            n: Node<K, V>
        ): Element<K, V> {
            var i = Nat32.fromNat(0);
            var elm = n.elements.head;
            label l while(i < minElements) {
                switch(elm) {
                    case null {
                        break l;
                    };
                    case (?e) {
                        elm := e.next;
                        i += 1;
                    };
                };
            };

            switch(elm) {
                case null {
                    assert false; loop { };
                };
                case (?e) {
                    return e;
                };
            };            
        };

        func _updateOwner(
            node: Node<K, V>
        ) {
            var elm = node.elements.head;
            label l loop {
                switch(elm) {
                    case null {
                        break l;
                    };
                    case (?e) {
                        e.owner := node;
                        elm := e.next;
                    };
                };
            };
        };

        func _updateOwnerAndParent(
            node: Node<K, V>
        ) {
            var elm = node.elements.head;
            label l loop {
                switch(elm) {
                    case null {
                        break l;
                    };
                    case (?e) {
                        e.owner := node;
                        switch(e.left) {
                            case null {
                                assert false; loop { };
                            };
                            case (?left) {
                                left.parent := ?node;
                            };
                        };
                        elm := e.next;
                    };
                };
            };
            
            switch(node.last) {
                case null {
                    assert false; loop { };
                };
                case (?last) {
                    last.parent := ?node;
                };
            };
        };

        func _splitNode(
            a: Node<K, V>
        ) {
            let s = _findSplitKey(a);

            var parent = switch(a.parent) {
                case null {
                    let p = Node<K, V>(_nextId(), false, null);
                    a.parent := ?p;
                    root := p;
                    p;
                };
                case (?p) {
                    p;
                };
            };

            let b = Node<K, V>(_nextId(), a.isLeaf, ?parent);

            b.prev := ?a;
            b.next := a.next;
            b.elements.head := s.next;
            b.elements.tail := a.elements.tail;
            b.elements.cnt := Nat32.sub(Nat32.sub(a.elements.cnt, minElements), 1);
            b.last := a.last;

            switch(a.next) {
                case null {
                };
                case (?next) {
                    next.prev := ?b;
                };
            };
            a.next := ?b;
            a.last := s.left;
            a.elements.tail := s.prev;
            a.elements.cnt := minElements;

            switch(s.next) {
                case null {
                    assert false; loop { };
                };
                case (?next) {
                    next.prev := null;
                };
            };
            switch(s.prev) {
                case null {
                    assert false; loop { };
                };
                case (?prev) {
                    prev.next := null;
                };
            };
            
            _updateOwnerAndParent(b);

            let e = _addElement(s.key, parent, null);
            e.left := ?a;
            switch(e.next) {
                case null {
                    parent.last := ?b;
                };
                case (?next) {
                    next.left := ?b;
                };
            };

            if(Nat32.greater(parent.elements.cnt, maxElements)) {
                _splitNode(parent);
            };
        };

        func _splitLeaf(
            a: Node<K, V>
        ) {
            let s = _findSplitKey(a);

            var parent = switch(a.parent) {
                case null {
                    let p = Node<K, V>(_nextId(), false, null);
                    a.parent := ?p;
                    root := p;
                    p;
                };
                case (?p) {
                    p;
                };
            };

            let b = Node<K, V>(_nextId(), true, ?parent);

            b.prev := ?a;
            b.next := a.next;
            b.elements.head := ?s;
            b.elements.tail := a.elements.tail;
            b.elements.cnt := Nat32.sub(a.elements.cnt, minElements);

            switch(a.next) {
                case null {
                    tail := b;
                };
                case (?next) {
                    next.prev := ?b;
                };
            };
            a.next := ?b;
            a.elements.tail := s.prev;
            a.elements.cnt := minElements;

            switch(s.prev) {
                case null {
                    assert false; loop { };
                };
                case (?prev) {
                    prev.next := null;
                };
            };
            s.prev := null;
            
            _updateOwner(b);

            let e = _addElement(s.key, parent, null);
            e.left := ?a;
            switch(e.next) {
                case null {
                    parent.last := ?b;
                };
                case (?next) {
                    next.left := ?b;
                };
            };

            if(Nat32.greater(parent.elements.cnt, maxElements)) {
                _splitNode(parent);
            };            
        };

        func _findLeftSiblingElement(
            node: Node<K, V> 
        ): ?Element<K, V> {
            var elm = switch(node.parent) {
                case null {
                    return null;
                };
                case (?parent) {
                    parent.elements.head;
                };
            };

            var prev: ?Element<K, V> = null;
            label l loop {
                switch(elm) {
                    case null {
                        break l;
                    };
                    case (?e) {
                        switch(e.left) {
                            case null {
                            };
                            case (?left) {
                                if(left.id == node.id) {
                                    return prev;
                                };

                                prev := elm;
                                elm := e.next;
                            };
                        };
                    };
                };
            };

            return prev;
        };

        func _mergeNodes(
            node_: Node<K, V>,
            sibling_: Node<K, V>,
            leftSiblingElm: ?Element<K, V>,
            parentElm: Element<K, V>
        ) {
            var sibling = sibling_;
            var node = node_;

            // merging with the right sibling?
            if(Option.isNull(leftSiblingElm)) {
                sibling := node_;
                node := sibling_;
            };

            // internal node?
            if(not node.isLeaf) {
                let clone = parentElm.clone();
                sibling.elements.add(clone, null);
                clone.left := sibling.last;
                clone.owner := sibling; 
                switch(clone.left) {
                    case null {
                        assert false; loop { };
                    };
                    case (?left) {
                        left.parent := ?sibling;
                        if(not left.isLeaf) {
                            _updateOwnerAndParent(left);
                        }
                        else {
                            _updateOwner(left);
                        };
                    };
                };

                // add all node's elements to sibling
                var elm = node.elements.head;
                label l loop {
                    switch(elm) {
                        case null {
                            break l;
                        };
                        case (?e) {
                            let next = e.next;
                            sibling.elements.add(e, null);
                            e.owner := sibling;
                            switch(e.left) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?left) {
                                    left.parent := ?sibling;
                                };
                            };
                            
                            elm := next;
                        };
                    };
                };
                
                // corrent last child
                let last = node.last;
                switch(last) {
                    case null {
                        assert false; loop { };
                    };
                    case (?l) {
                        l.parent := ?sibling;
                    };
                };
                sibling.last := last;
            }
            // leaf..
            else {
                // add all node's elements to sibling
                var elm = node.elements.head;
                label l loop {
                    switch(elm) {
                        case null {
                            break l;
                        };
                        case (?e) {
                            let next = e.next;
                            sibling.elements.add(e, null);
                            e.owner := sibling;
                            elm := next;
                        };
                    };
                };
            };

            // remove node
            switch(node.prev) { 
                case null {
                    if(node.isLeaf) {
                        switch(node.next) {
                            case null {
                                assert false; loop { };
                            };
                            case (?next) {
                                head := next;
                            };
                        };                        
                    };
                };
                case (?prev) {
                    prev.next := node.next;
                };
            };

            switch(node.next) { 
                case null {
                    if(node.isLeaf) {
                        switch(node.prev) {
                            case null {
                                assert false; loop { };
                            };
                            case (?prev) {
                                tail := prev;
                            };
                        };
                    };
                };
                case (?next) {
                    next.prev := node.prev;
                };
            };
            
            // remove parent element from node's parent
            _deleteElement(parentElm);

            // delete node
            node.elements := ElementList();
        };

        func _redisNodes(
            node: Node<K, V>,
            sibling: Node<K, V>,
            leftSiblingElm: ?Element<K, V>,
            parentElm: Element<K, V>
        ) {
            // take the last element from the left sibling
            if(Option.isSome(leftSiblingElm)) {
                switch(sibling.elements.tail) {
                    case null {
                        assert false; loop { };
                    };
                    case (?tail) {
                        switch(tail.prev) {
                            case null {
                            };
                            case (?prev) {
                                prev.next := null;
                            };
                        };

                        sibling.elements.tail := tail.prev;

                        let head = node.elements.head;

                        if(not node.isLeaf) {
                            // swap keys
                            let tmp1 = tail.key;
                            tail.key := parentElm.key;
                            parentElm.key := tmp1;
                                
                            // swap children
                            let tmp2 = sibling.last;
                            sibling.last := tail.left;
                            switch(sibling.last) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?sl) {
                                    sl.parent := ?sibling;
                                };
                            };
                            tail.left := tmp2;
                        }
                        else {
                            parentElm.key := tail.key;
                        };

                        tail.prev := null;
                        tail.next := head;
                        tail.owner := node;
                        if(not node.isLeaf) {
                            switch(tail.left) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?left) {
                                    left.parent := ?node;
                                };
                            };
                        };

                        node.elements.head := ?tail;
                        switch(head) {
                            case null {
                                node.elements.tail := ?tail;
                            };
                            case (?h) {
                                h.prev := ?tail;
                            };
                        };
                    };
                };
            }
            // take the first element from the right sibling
            else {
                switch(sibling.elements.head) {
                    case null {
                        assert false; loop { };
                    };
                    case (?head) {
                        switch(head.next) {
                            case null {
                            };
                            case (?next) {
                                next.prev := null;
                            };
                        };
                        sibling.elements.head := head.next;
                            
                        let tail = node.elements.tail;
                        
                        if(not node.isLeaf) {
                            // swap keys
                            let tmp1 = head.key;
                            head.key := parentElm.key;
                            parentElm.key := tmp1;

                            // swap children
                            let tmp2 = node.last;
                            node.last := head.left;
                            switch(node.last) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?nl) {
                                    nl.parent := ?node;
                                };
                            };
                            
                            head.left := tmp2;
                        }
                        else {
                            switch(head.next) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?next) {
                                    parentElm.key := next.key;
                                };
                            };
                        };
                        
                        head.prev := tail;
                        head.next := null;
                        head.owner := node;
                        if(not node.isLeaf) {
                            switch(head.left) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?left) {
                                    left.parent := ?node;
                                };
                            };
                        };
                        
                        node.elements.tail := ?head;
                        switch(tail) {
                            case null {
                                node.elements.head := ?head;
                            };
                            case (?tail) {
                                tail.next := ?head;
                            };
                        };
                    };
                };
            };

            sibling.elements.cnt -= 1;
            node.elements.cnt += 1;
        };

        func _deleteElementFromRoot(
            elm: Element<K, V>
        ) {
            let node = root;
            
            switch(node.elements.head)
            {
                case null {
                    assert false; loop { };
                };
                case (?h) {
                    let child = h.left;
                    
                    node.elements.del(elm);
                    // any element left? do nothing..
                    if(Nat32.greater(node.elements.cnt, 0)) {
                        return;
                    };
                    
                    // no elements and it's a leaf? 
                    if(node.isLeaf) {
                        // create a new root leaf
                        root := Node<K, V>(_nextId(), true, null);
                        head := root;
                        tail := root;
                    }
                    // make child as root
                    else {
                        switch(child) {
                            case null {
                                assert false; loop { };
                            };
                            case (?child) {
                                root := child;
                                child.parent := null;
                            };
                        };
                    };
                    
                    // remove node
                    switch(node.prev) { 
                        case null {
                        };
                        case (?prev) {
                            prev.next := node.next;
                        };
                    };

                    switch(node.next) { 
                        case null {
                        };
                        case (?next) {
                            next.prev := node.prev;
                        };
                    };

                    // delete node    
                    node.elements := ElementList();
                };
            };
        };

        func _deleteElement(
            elm: Element<K, V>
        ) {
            let node = elm.owner;

            // not a leaf?
            if(not node.isLeaf) {
                // shift children pointers
                switch(elm.next) {
                    case null {
                        node.last := elm.left;
                    };
                    case (?next) {
                        next.left := elm.left;
                    };
                };
            };

            // is the element at root?
            if(Option.isNull(node.parent)) {
                _deleteElementFromRoot(elm);
                return;
            };

            // remove element from node's element list
            node.elements.del(elm);
            // enough elements left? do nothing.. 
            if(Nat32.greaterOrEqual(node.elements.cnt, minElements)) {
                return;
            };

            // find node's left sibling (if any)
            let leftSiblingElm = _findLeftSiblingElement(node);
            let parentElm = switch(leftSiblingElm) {
                case null {
                    switch(node.parent) {
                        case null {
                            assert false; loop { };
                        };
                        case (?parent) {
                            switch(parent.elements.head)
                            {
                                case null {
                                    assert false; loop { };
                                };
                                case (?head) {
                                    head;
                                };
                            };
                        };
                    };
                };
                case (?elm) {
                    elm;
                };
            };

            let sibling = switch(leftSiblingElm) {
                case null {
                    switch(node.parent) {
                        case null {
                            assert false; loop { };
                        };
                        case (?parent) {
                            switch(parent.elements.head) {
                                case null {
                                    assert false; loop { };
                                };
                                case (?head) {
                                    switch(head.next) {
                                        case null {
                                            switch(parent.last) {
                                                case null {
                                                    assert false; loop { };
                                                };
                                                case (?last) {
                                                    last;
                                                };
                                            };
                                        };
                                        case (?next) {
                                            switch(next.left) {
                                                case null {
                                                    assert false; loop { };
                                                };
                                                case (?left) {
                                                    left;
                                                };
                                            };
                                        };
                                    };
                                };
                            };
                        };
                    };
                };
                case (?elm) {
                    switch(elm.left) {
                        case null {
                            assert false; loop { };
                        };
                        case (?left) {
                            left;
                        };
                    };
                };
            };
            
            // if it's not a leaf, max is one left
            let maxElms = switch(node.isLeaf) {
                case true {
                    maxElements;
                };
                case false {
                    Nat32.sub(maxElements, 1);
                };
            };
            
            if(Nat32.lessOrEqual(
                Nat32.add(sibling.elements.cnt, node.elements.cnt), 
                maxElms)) {
                _mergeNodes(node, sibling, leftSiblingElm, parentElm);
            }
            else {
                _redisNodes(node, sibling, leftSiblingElm, parentElm);
            };
        };

        ///
        public func delete(
            key: K 
        ) {
            let l = _findLeaf(key);
            switch(_findElement(l, key)) {
                case null {
                };
                case (?e) {
                    _deleteElement(e);
                };
            };
        };
    };
};
