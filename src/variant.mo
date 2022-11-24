/**
 * Variant type implementation
 * Copyright 2021 André Vicentini (https://github.com/av1ctor/)
 * Licensed under the Apache license Version 2.0
 */

import Text "mo:base/Text";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Hash "mo:base/Hash";
import HashMap "mo:base/HashMap";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Int "mo:base/Int";
import Int8 "mo:base/Int8";
import Int16 "mo:base/Int16";
import Int32 "mo:base/Int32";
import Int64 "mo:base/Int64";
import Float "mo:base/Float";
import Bool "mo:base/Bool";
import Blob "mo:base/Blob";
import Order "mo:base/Order";
import Iter "mo:base/Iter";

module {
    public type MapEntry = {
        key: Text;
        value: Variant;
    };

    public type Variant = {
        #nil;
        #text: Text;
        #nat: Nat;
        #nat8: Nat8;
        #nat16: Nat16;
        #nat32: Nat32;
        #nat64: Nat64;
        #int: Int;
        #int8: Int8;
        #int16: Int16;
        #int32: Int32;
        #int64: Int64;
        #float: Float;
        #bool: Bool;
        #blob: Blob;
        #array: [Variant];
        #map: [MapEntry];
        #tuple: (Variant, Variant);
    };

    public func compare(
        a: Variant, 
        b: Variant
    ): Order.Order {
        switch(a, b) {
            case (#text(a), #text(b)) {
                return Text.compare(a, b);
            };
            case (#nat(a), #nat(b)) {
                return Nat.compare(a, b);
            };
            case (#nat8(a), #nat8(b)) {
                return Nat8.compare(a, b);
            };
            case (#nat16(a), #nat16(b)) {
                return Nat16.compare(a, b);
            };
            case (#nat32(a), #nat32(b)) {
                return Nat32.compare(a, b);
            };
            case (#nat64(a), #nat64(b)) {
                return Nat64.compare(a, b);
            };
            case (#int(a), #int(b)) {
                return Int.compare(a, b);
            };
            case (#int8(a), #int8(b)) {
                return Int8.compare(a, b);
            };
            case (#int16(a), #int16(b)) {
                return Int16.compare(a, b);
            };
            case (#int32(a), #int32(b)) {
                return Int32.compare(a, b);
            };
            case (#int64(a), #int64(b)) {
                return Int64.compare(a, b);
            };
            case (#float(a), #float(b)) {
                return Float.compare(a, b);
            };
            case (#bool(a), #bool(b)) {
                return Bool.compare(a, b);
            };
            case (#blob(a), #blob(b)) {
                return Blob.compare(a, b);
            };
            case (#array(a), #array(b)) {
                if(a.size() != b.size()) {
                    return if(a.size() < b.size()) 
                        #less
                    else
                        #greater;
                };

                if(a.size() > 0) {
                    for(i in Iter.range(0, a.size() - 1)) {
                        let av = a[i];
                        let bv = b[i];
                        let res = compare(av, bv);
                        if(res != #equal) {
                            return res;
                        };
                    };
                };

                return #equal;
            };
            case(#map(a), #map(b)) {
                if(a.size() != b.size()) {
                    return if(a.size() < b.size()) 
                        #less
                    else
                        #greater;
                };

                if(a.size() > 0) {
                    for(i in Iter.range(0, a.size() - 1)) {
                        let av = a[i];
                        switch(Array.find(b, func(kv: MapEntry): Bool = kv.key == av.key)) {
                            case null {
                                return #greater;
                            };
                            case (?bv) {
                                let res = compare(av.value, bv.value);
                                if(res != #equal) {
                                    return res;
                                };
                            };
                        };
                    };
                };

                return #equal;
            };
            case (#tuple(a), #tuple(b)) {
                switch(compare(a.0, b.0), compare(a.1, b.1)) {
                    case (#equal, #equal) {
                        return #equal;
                    };
                    case (#equal, other) {
                        return other;
                    };
                    case (#less, _) {
                        return #less;
                    };
                    case (#greater, _) {
                        return #greater;
                    };
                };
            };
            case (#nil, #nil) {
                return #equal;
            };
            case (#nil, _) {
                return #less;
            };
            case (_, #nil) {
                return #greater;
            };
            case (_, _) {
                assert false; loop {};
            };
        };
    };

    public func cmp(
        a: Variant, 
        b: Variant
    ): Int {
        switch(compare(a, b)) {
            case (#less) {
                return -1;
            };
            case (#equal) {
                return 0;
            };
            case (#greater) {
                return 1;
            };
        };
    };

    public func equal(
        a: Variant, 
        b: Variant
    ): Bool {
        switch(compare(a, b)) {
            case (#equal) {
                return true;
            };
            case _ {
                return false;
            };
        };
    };

    public func hash(
        a: Variant
    ): Hash.Hash {
        switch(a) {
            case (#text(a)) {
                return Text.hash(a);
            };
            case (#nat(a)) {
                return Nat32.fromNat(a);
            };
            case (#nat8(a)) {
                return Nat32.fromNat(Nat8.toNat(a));
            };
            case (#nat16(a)) {
                return Nat32.fromNat(Nat16.toNat(a));
            };
            case (#nat32(a)) {
                return a;
            };
            case (#nat64(a)) {
                return Nat32.fromNat(Nat64.toNat(a));
            };
            case (#int(a)) {
                return Nat32.fromNat(Int.abs(a));
            };
            case (#int8(a)) {
                return Nat32.fromNat(Int.abs(Int8.toInt(a)));
            };
            case (#int16(a)) {
                return Nat32.fromNat(Int.abs(Int16.toInt(a)));
            };
            case (#int32(a)) {
                return Nat32.fromNat(Int.abs(Int32.toInt(a)));
            };
            case (#int64(a)) {
                return Nat32.fromNat(Int.abs(Int64.toInt(a)));
            };
            case (#float(a)) {
                return Text.hash(Float.format(#fix(8), a));
            };
            case (#bool(a)) {
                return if(a) 1 else 0;
            };
            case (#blob(a)) {
                return Blob.hash(a);
            };
            case (#nil) {
                return 0;
            };
            case (_) {
                assert false; loop {};
            };
        };
    };

    public func isTypeOfEqual(
        a: Variant, 
        b: Variant
    ): Bool {
        switch(a, b) {
            case (#text(_), #text(_)) {
                return true;
            };
            case (#nat(_), #nat(_)) {
                return true;
            };
            case (#nat8(_), #nat8(_)) {
                return true;
            };
            case (#nat16(_), #nat16(_)) {
                return true;
            };
            case (#nat32(_), #nat32(_)) {
                return true;
            };
            case (#nat64(_), #nat64(_)) {
                return true;
            };
            case (#int(_), #int(_)) {
                return true;
            };
            case (#int8(_), #int8(_)) {
                return true;
            };
            case (#int16(_), #int16(_)) {
                return true;
            };
            case (#int32(_), #int32(_)) {
                return true;
            };
            case (#int64(_), #int64(_)) {
                return true;
            };
            case (#float(_), #float(_)) {
                return true;
            };
            case (#bool(_), #bool(_)) {
                return true;
            };
            case (#blob(_), #blob(_)) {
                return true;
            };
            case (#array(_), #array(_)) {
                return true;
            };
            case(#map(_), #map(_)) {
                return true;
            };
            case (#tuple(_), #tuple(_)) {
                return true;
            };
            case (#nil, #nil) {
                return true;
            };
            case (_, _) {
                return false
            };
        };
    };

    public func toText(
        a: Variant
    ): Text {
        switch(a) {
            case (#text(a)) {
                return a;
            };
            case (#nat(a)) {
                return Nat.toText(a);
            };
            case (#nat8(a)) {
                return Nat8.toText(a);
            };
            case (#nat16(a)) {
                return Nat16.toText(a);
            };
            case (#nat32(a)) {
                return Nat32.toText(a);
            };
            case (#nat64(a)) {
                return Nat64.toText(a);
            };
            case (#int(a)) {
                return Int.toText(a);
            };
            case (#int8(a)) {
                return Int8.toText(a);
            };
            case (#int16(a)) {
                return Int16.toText(a);
            };
            case (#int32(a)) {
                return Int32.toText(a);
            };
            case (#int64(a)) {
                return Int64.toText(a);
            };
            case (#float(a)) {
                return Float.toText(a);
            };
            case (#bool(a)) {
                return if(a) "true" else "false";
            };
            case (#blob(a)) {
                return Nat32.toText(Blob.hash(a));
            };
            case (#nil) {
                return "null";
            };
            case (_) {
                assert false; loop {};
            };
        };
    };

    public func getText(
        v: Variant
    ): Text {
        switch(v) {
            case (#text(val)) val;
            case _ "";
        };
    };

    public func getOptText(
        v: ?Variant
    ): Text {
        switch(v) {
            case (?#text(val)) val;
            case _ "";
        };
    };

    public func getOptTextOpt(
        v: ?Variant
    ): ?Text {
        switch(v) {
            case (?#text(val)) ?val;
            case _ null;
        };
    };

    public func getNat(
        v: Variant
    ): Nat {
        switch(v) {
            case (#nat(val)) val;
            case _ 0;
        };
    };

    public func getOptNat(
        v: ?Variant
    ): Nat {
        switch(v) {
            case (?#nat(val)) val;
            case _ 0;
        };
    };

    public func getOptNatOpt(
        v: ?Variant
    ): ?Nat {
        switch(v) {
            case (?#nat(val)) ?val;
            case _ null;
        };
    };

    public func getNat8(
        v: Variant
    ): Nat8 {
        switch(v) {
            case (#nat8(val)) val;
            case _ 0;
        };
    };

    public func getOptNat8(
        v: ?Variant
    ): Nat8 {
        switch(v) {
            case (?#nat8(val)) val;
            case _ 0;
        };
    };

    public func getOptNat8Opt(
        v: ?Variant
    ): ?Nat8 {
        switch(v) {
            case (?#nat8(val)) ?val;
            case _ null;
        };
    };

    public func getNat16(
        v: Variant
    ): Nat16 {
        switch(v) {
            case (#nat16(val)) val;
            case _ 0;
        };
    };

    public func getOptNat16(
        v: ?Variant
    ): Nat16 {
        switch(v) {
            case (?#nat16(val)) val;
            case _ 0;
        };
    };

    public func getOptNat16Opt(
        v: ?Variant
    ): ?Nat16 {
        switch(v) {
            case (?#nat16(val)) ?val;
            case _ null;
        };
    };

    public func getNat32(
        v: Variant
    ): Nat32 {
        switch(v) {
            case (#nat32(val)) val;
            case _ 0;
        };
    };

    public func getOptNat32(
        v: ?Variant
    ): Nat32 {
        switch(v) {
            case (?#nat32(val)) val;
            case _ 0;
        };
    };    

    public func getOptNat32Opt(
        v: ?Variant
    ): ?Nat32 {
        switch(v) {
            case (?#nat32(val)) ?val;
            case _ null;
        };
    };    

    public func getNat64(
        v: Variant
    ): Nat64 {
        switch(v) {
            case (#nat64(val)) val;
            case _ 0;
        };
    };

    public func getOptNat64(
        v: ?Variant
    ): Nat64 {
        switch(v) {
            case (?#nat64(val)) val;
            case _ 0;
        };
    };

    public func getOptNat64Opt(
        v: ?Variant
    ): ?Nat64 {
        switch(v) {
            case (?#nat64(val)) ?val;
            case _ null;
        };
    };

    public func getInt(
        v: Variant
    ): Int {
        switch(v) {
            case (#int(val)) val;
            case _ 0;
        };
    };

    public func getOptInt(
        v: ?Variant
    ): Int {
        switch(v) {
            case (?#int(val)) val;
            case _ 0;
        };
    };        

    public func getOptIntOpt(
        v: ?Variant
    ): ?Int {
        switch(v) {
            case (?#int(val)) ?val;
            case _ null;
        };
    };        

    public func getInt8(
        v: Variant
    ): Int8 {
        switch(v) {
            case (#int8(val)) val;
            case _ 0;
        };
    };

    public func getOptInt8(
        v: ?Variant
    ): Int8 {
        switch(v) {
            case (?#int8(val)) val;
            case _ 0;
        };
    };

    public func getOptInt8Opt(
        v: ?Variant
    ): ?Int8 {
        switch(v) {
            case (?#int8(val)) ?val;
            case _ null;
        };
    };

    public func getInt16(
        v: Variant
    ): Int16 {
        switch(v) {
            case (#int16(val)) val;
            case _ 0;
        };
    };

    public func getOptInt16(
        v: ?Variant
    ): Int16 {
        switch(v) {
            case (?#int16(val)) val;
            case _ 0;
        };
    };

    public func getOptInt16Opt(
        v: ?Variant
    ): ?Int16 {
        switch(v) {
            case (?#int16(val)) ?val;
            case _ null;
        };
    };

    public func getInt32(
        v: Variant
    ): Int32 {
        switch(v) {
            case (#int32(val)) val;
            case _ 0;
        };
    };

    public func getOptInt32(
        v: ?Variant
    ): Int32 {
        switch(v) {
            case (?#int32(val)) val;
            case _ 0;
        };
    };    

    public func getOptInt32Opt(
        v: ?Variant
    ): ?Int32 {
        switch(v) {
            case (?#int32(val)) ?val;
            case _ null;
        };
    };    

    public func getInt64(
        v: Variant
    ): Int64 {
        switch(v) {
            case (#int64(val)) val;
            case _ 0;
        };
    };

    public func getOptInt64(
        v: ?Variant
    ): Int64 {
        switch(v) {
            case (?#int64(val)) val;
            case _ 0;
        };
    };

    public func getOptInt64Opt(
        v: ?Variant
    ): ?Int64 {
        switch(v) {
            case (?#int64(val)) ?val;
            case _ null;
        };
    };

    public func getFloat(
        v: Variant
    ): Float {
        switch(v) {
            case (#float(val)) val;
            case _ 0.0;
        };
    };

    public func getOptFloat(
        v: ?Variant
    ): Float {
        switch(v) {
            case (?#float(val)) val;
            case _ 0.0;
        };
    };

    public func getOptFloatOpt(
        v: ?Variant
    ): ?Float {
        switch(v) {
            case (?#float(val)) ?val;
            case _ null;
        };
    };

    public func getBool(
        v: Variant
    ): Bool {
        switch(v) {
            case (#bool(val)) val;
            case _ false;
        };
    };

    public func getOptBool(
        v: ?Variant
    ): Bool {
        switch(v) {
            case (?#bool(val)) val;
            case _ false;
        };
    };

    public func getOptBoolOpt(
        v: ?Variant
    ): ?Bool {
        switch(v) {
            case (?#bool(val)) ?val;
            case _ null;
        };
    };

    let emptyArray: [Nat8] = [];

    public func getBlob(
        v: Variant
    ): Blob {
        switch(v) {
            case (#blob(val)) val;
            case _ Blob.fromArray(emptyArray);
        };
    };

    public func getOptBlob(
        v: ?Variant
    ): Blob {
        switch(v) {
            case (?#blob(val)) val;
            case _ Blob.fromArray(emptyArray);
        };
    };

    public func getOptBlobOpt(
        v: ?Variant
    ): ?Blob {
        switch(v) {
            case (?#blob(val)) ?val;
            case _ null;
        };
    };

    public func getArray(
        v: Variant
    ): [Variant] {
        switch(v) {
            case (#array(val)) val;
            case _ [];
        };
    };

    public func getOptArray(
        v: ?Variant
    ): [Variant] {
        switch(v) {
            case (?#array(val)) val;
            case _ [];
        };
    };

    public func getOptArrayOpt(
        v: ?Variant
    ): ?[Variant] {
        switch(v) {
            case (?#array(val)) ?val;
            case _ null;
        };
    };

    public func mapToHashMap(
        map: [MapEntry]
    ): HashMap.HashMap<Text, Variant> {
        let hm = HashMap.HashMap<Text, Variant>(map.size(), Text.equal, Text.hash);
        for(e in map.vals()) {
            hm.put(e.key, e.value);
        };
        hm;
    };

    public func hashMapToMap(
        hm: HashMap.HashMap<Text, Variant>
    ): [MapEntry] {
        let map = Buffer.Buffer<MapEntry>(hm.size());
        for(e in hm.entries()) {
            map.add({key = e.0; value = e.1});
        };
        Buffer.toArray(map);
    };

    public func getMap(
        v: Variant
    ): [MapEntry] {
        switch(v) {
            case (#map(val)) val;
            case _ [];
        };
    };

    public func getOptMap(
        v: ?Variant
    ): [MapEntry] {
        switch(v) {
            case (?#map(val)) val;
            case _ [];
        };
    };

    public func getOptMapOpt(
        v: ?Variant
    ): ?[MapEntry] {
        switch(v) {
            case (?#map(val)) ?val;
            case _ null;
        };
    };

    public func getMapAsHM(
        v: Variant
    ): HashMap.HashMap<Text, Variant> {
        switch(v) {
            case (#map(val)) mapToHashMap(val);
            case _ HashMap.HashMap<Text, Variant>(0, Text.equal, Text.hash);
        };
    };

    public func getOptMapAsHM(
        v: ?Variant
    ): HashMap.HashMap<Text, Variant> {
        switch(v) {
            case (?#map(val)) mapToHashMap(val);
            case _ HashMap.HashMap<Text, Variant>(0, Text.equal, Text.hash);
        };
    };

    public func getOptMapOptAsHM(
        v: ?Variant
    ): ?HashMap.HashMap<Text, Variant> {
        switch(v) {
            case (?#map(val)) ?mapToHashMap(val);
            case _ null;
        };
    };
};