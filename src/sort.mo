// Adapted from https://github.com/dfinity/motoko-base
// Released under the Apache-2.0 License

import Nat "mo:base/Nat";
import Buffer "mo:base/Buffer";
import Array "mo:base/Array";

module {    
    public func sort<A>(
        xs : Buffer.Buffer<A>, 
        cmp : (A, A) -> Int
    ) {
        if (xs.size() < 2) {
            return;
        };

        let aux : [var A] = Array.tabulateVar<A>(xs.size(), func i { xs.get(i) });

        func merge(lo : Nat, mid : Nat, hi : Nat) {
            var i = lo;
            var j = mid + 1;
            var k = lo;
            while(k <= hi) {
                aux[k] := xs.get(k);
                k += 1;
            };
            k := lo;
            while(k <= hi) {
                if (i > mid) {
                    xs.put(k, aux[j]);
                    j += 1;
                } 
                else if (j > hi) {
                    xs.put(k, aux[i]);
                    i += 1;
                } 
                else if (cmp(aux[j], aux[i]) < 0) {
                    xs.put(k, aux[j]);
                    j += 1;
                } 
                else {
                    xs.put(k, aux[i]);
                    i += 1;
                };
                k += 1;
            };
        };

        func go(lo : Nat, hi : Nat) {
            if (hi <= lo) return;
            let mid : Nat = lo + (hi - lo) / 2;
            go(lo, mid);
            go(mid + 1, hi);
            merge(lo, mid, hi);
        };
    
        go(0, xs.size() - 1);
    };
};