/**
 * Table implementation
 * Copyright 2021-2022 André Vicentini (https://github.com/av1ctor/)
 * Licensed under the Apache license Version 2.0
 */

import Array "mo:base/Array";
import Text "mo:base/Text";
import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Buffer "mo:base/Buffer";
import HashMap "mo:base/HashMap";
import TrieMap "mo:base/TrieMap";
import Hash "mo:base/Hash";
import Set "mo:base/TrieSet";
import Iter "mo:base/Iter";
import Option "mo:base/Option";
import Result "mo:base/Result";
import Order "mo:base/Order";
import D "mo:base/Debug";
import Variant "./variant";
import BPlusTree "./bplustree";
import SuffixTree "./suffixtree";
import Sort "./sort";
import Utils "./utils";

module {
    public type ColumnOption = {
        #primary;
        #unique;
        #sortable;
        #nullable;
        #partial;
        #prefixed;
        #multiple;
		#min: Nat;
		#max: Nat;
    };

    public type ColumnRequest = {
        name: Text;
        options: [ColumnOption];
    };

    public type Schema = {
        name: Text;
        version: Float;
        columns: [ColumnRequest];
    };

    type Column = {
        name: Text;
        multiple: Bool;
        primary: Bool;
        unique: Bool;
        sortable: Bool;
        nullable: Bool;
        partial: Bool;
        prefixed: Bool;
		min: Nat;
		max: Nat;
    };

    type Id = Nat32;
    
    public type CriteriaOp = {
        #eq;
        #contains;
        #startsWith;
        #neq;
        #lt;
        #lte;
        #gt;
        #gte;
        #between;
    };
    
    public type Criteria = {
        key: Text;
        op: CriteriaOp;
        value: Variant.Variant;
    };

    public type SortDirection = {
        #asc;
        #desc;
    };

    public type SortBy<E> = {
        key: Text;
        dir: SortDirection;
        cmp: (E, E) -> Int;
    };

    public type Limit = {
        offset: Nat;
        size: Nat;
    };

    /*public type Join<E> = {
        table: Table<E>;
        prefix: Text;
        criterias: [Criteria];
    };*/

    public class Table<E>(
        schema: Schema, 
        serialize: (E, Bool) -> HashMap.HashMap<Text, Variant.Variant>,
        deserialize: (HashMap.HashMap<Text, Variant.Variant>) -> E//,
        //getJoined: (E, Text, E) -> E
    ) {
        let columns = HashMap.HashMap<Text, Column>(
            schema.columns.size(), Text.equal, Text.hash
        );
        let rows = Buffer.Buffer<?E>(
            100
        );
        let uniqIndexes = HashMap.HashMap<Text, BPlusTree.BPlusTree<Variant.Variant, Id>>(
            schema.columns.size(), Text.equal, Text.hash
        );
        let multIndexes = HashMap.HashMap<Text, BPlusTree.BPlusTree<Variant.Variant, Set.Set<Id>>>(
            schema.columns.size(), Text.equal, Text.hash
        );
        let partIndexes = HashMap.HashMap<Text, SuffixTree.SuffixTree<Id>>(
            schema.columns.size(), Text.equal, Text.hash
        );
        let prefixIndexes = HashMap.HashMap<Text, TrieMap.TrieMap<Text, Set.Set<Id>>>(
            schema.columns.size(), Text.equal, Text.hash
        );
        let nullUniqIndexes = HashMap.HashMap<Text, ?Id>(
            schema.columns.size(), Text.equal, Text.hash
        );
        let nullMultIndexes = HashMap.HashMap<Text, Set.Set<Id>>(
            schema.columns.size(), Text.equal, Text.hash
        );

        func _cmpText(a: Text, b: Text): Int {
            switch(Text.compare(a, b)) {
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

        func _hashId(id: Id): Hash.Hash {
            return id;
        };

        func _cmpId(a: Id, b: Id): Int {
            if(a < b) {
                return -1;
            }
            else if(a == b) {
                return 0;
            }
            else {
                return 1;
            };
        };

        func _cmpIdOrder(a: Id, b: Id): Order.Order {
            if(a < b) {
                return #less;
            }
            else if(a == b) {
                return #equal;
            }
            else {
                return #greater;
            };
        };

        func _cmpIdReverseOrder(a: Id, b: Id): Order.Order {
            if(a < b) {
                return #greater;
            }
            else if(a == b) {
                return #equal;
            }
            else {
                return #less;
            };
        };

        func _equalId(a: Id, b: Id): Bool {
            return a == b;
        };

        func _allocIndexes(
            col: Column
        ) {
            // skip primary
            if(col.primary) {
                return;
            };
            
            if(col.unique) {
                uniqIndexes.put(
                    col.name, 
                    BPlusTree.BPlusTree<Variant.Variant, Id>(5, Variant.cmp));
                nullUniqIndexes.put(col.name, null);
            }
            else if(col.sortable) {
                multIndexes.put(
                    col.name, 
                    BPlusTree.BPlusTree<Variant.Variant, Set.Set<Id>>(5, Variant.cmp));
                nullMultIndexes.put(col.name, Set.empty<Id>());
            };

            if(col.prefixed) {
                prefixIndexes.put(
                    col.name, 
                    TrieMap.TrieMap<Text, Set.Set<Id>>(
                        Text.equal, 
                        Text.hash
                    )
                );
            };

            if(col.partial) {
                partIndexes.put(
                    col.name, 
                    SuffixTree.SuffixTree<Id>(
                        _hashId, 
                        _equalId
                    )
                );
            };            
        };

        for (column in schema.columns.vals()) {
            var primary = false;
            var unique = false;
            var sortable = false;
            var nullable = false;
            var partial = false;
            var prefixed = false;
            var multiple = false;
			var min = 0;
			var max = 2**64;

            for (option in column.options.vals()) {
                switch(option) {
                    case (#primary) primary := true;
                    case (#unique) unique := true;
                    case (#sortable) sortable := true;
                    case (#nullable) nullable := true;
                    case (#partial) partial := true;
                    case (#prefixed) prefixed := true;
                    case (#multiple) multiple := true;
                    case (#min(val)) min := val;
                    case (#max(val)) max := val;
                };
            };

            let col = {
                name = column.name;
                primary = primary;
                unique = unique;
                sortable = sortable;
                nullable = nullable;
                partial = partial;
                prefixed = prefixed;
                multiple = multiple;
				min = min;
				max = max;
            };

            columns.put(col.name, col);
            
            _allocIndexes(col);
        };

        ///
        public func nextId(
        ): Id {
            return Nat32.fromNat(rows.size() + 1);
        };
        
        ///
        public func insert(
            _id: Nat32,
            entity: E
        ): Result.Result<Id, Text> {
            if(_id == 0) {
                return #err("Invalid id");
            };
            
            let map = serialize(entity, true);
			switch(_validate(map)) {
                case(#err(errors)) {
                    return #err(Text.join(",", errors.vals()));
                };
                case _ {
					switch(_canInsert(entity, map)) {
						case(#err(msg)) {
							return #err(msg);
						};
						case _ {
							rows.add(?entity);
							_insertIntoIndexes(_id, entity, map);
							return #ok(_id);
						};
					};
				};
			};
        };

        ///
        public func replace(
            _id: Id,
            entity: E
        ): Result.Result<(), Text> {
            if(_id == 0) {
                return #err("Invalid id");
            };
            
            // check if _id exists
            let current = switch(rows.getOpt(Nat32.toNat(_id) - 1)) {
                case null {
                    return #err("Primary key not found: " # Nat32.toText(_id))
                };
                case (?row) {
                    switch(row) {
                        case null {
                            return #err("Primary key not found: " # Nat32.toText(_id))
                        };
                        case (?row) {
                            row;
                        };
                    };
                };
            };

            let map = serialize(entity, true);
			switch(_validate(map)) {
                case(#err(errors)) {
                    return #err(Text.join(",", errors.vals()));
                };
                case _ {
					let currentMap = serialize(current, true);
					_deleteFromIndexes(_id, current, currentMap);
					
					switch(_canInsert(entity, map)) {
						case(#err(msg)) {
							// readd the old entity
							_insertIntoIndexes(_id, current, currentMap);
							#err(msg);
						};
						case _ {
							rows.put(Nat32.toNat(_id) - 1, ?entity);
							_insertIntoIndexes(_id, entity, map);
							#ok();
						};
					};
				};
			};
        };

        ///
        public func delete(
            _id: Id
        ): Result.Result<(), Text> {
            if(_id == 0) {
                return #err("Invalid id");
            };
            
            // check if pkey exists
            let entity = switch(rows.getOpt(Nat32.toNat(_id) - 1)) {
                case null {
                    return #err("Primary key not found: " # Nat32.toText(_id))
                };
                case (?row) {
                    switch(row) {
                        case null {
                            return #err("Primary key not found: " # Nat32.toText(_id))
                        };
                        case (?row) {
                            row;
                        };
                    };
                };
            };

            let map = serialize(entity, true);
            _deleteFromIndexes(_id, entity, map);

            // we can't remove entity since the id's after _id would be shifted back
            rows.put(Nat32.toNat(_id) - 1, null);

            #ok();
        };

        ///
        public func get(
            _id: Nat32
        ): Result.Result<?E, Text> {
            if(_id == 0) {
                return #err("Invalid id");
            };

            switch(rows.getOpt(Nat32.toNat(_id) - 1)) {
                case null {
                    return #err("Not found");
                };
                case (?row) {
                    return #ok(row);
                };
            };
        };

        ///
        public func findOne(
            criterias: [Criteria]
        ): Result.Result<?E, Text> {
            switch(find(?criterias, null, null/*, null*/)) {
                case (#err(msg)) {
                    return #err(msg);
                };
                case (#ok(arr)) {
                    if(arr.size() > 0) {
                        return #ok(?arr[0]);
                    }
                    else {
                        return #ok(null);
                    };
                };
            };
        };

        func _filterById(
            crit: Criteria,
            ids: Set.Set<Id>
        ): Result.Result<Set.Set<Id>, Text> {
            switch(crit.op) {
                case (#eq) {
                    switch(crit.value) {
                        case (#nil) {
                            return #err("Value can't be null on column _id");
                        };
                        case (#nat32(_id)) {
                            if(Set.size<Id>(ids) == 0) {
                                switch(rows.getOpt(Nat32.toNat(_id) - 1)) {
                                    case null {
                                        return #ok(Set.empty<Id>());
                                    };
                                    case _ {
                                        return #ok(Set.put<Id>(ids, _id, _hashId(_id), _equalId));
                                    };
                                };
                            }
                            else {
                                if(Set.mem<Id>(ids, _id, _hashId(_id), _equalId)) {
                                    return #ok(Set.put<Id>(Set.empty<Id>(), _id, _hashId(_id), _equalId));
                                }
                                else {
                                    return #ok(Set.empty<Id>());
                                }
                            };
                        };
                        case _ {
                            return #err("Type of column _id must be Nat32");
                        };
                    };
                };                          
                case _ {
                    return #err("Unsupported operator for column _id");
                };
            };
        };

        func _filterByEq(
            crit: Criteria,
            ids: Set.Set<Id>,
            col: Column
        ): Result.Result<Set.Set<Id>, Text> {
            if(not (col.unique or col.sortable)) {
                return #err("No index found for column " # crit.key);
            };

            var set = Set.empty<Id>();

            switch(crit.value) {
                case (#nil) {
                    if(col.unique) {
                        switch(nullUniqIndexes.get(crit.key)) {
                            case null {
                                return #err("No index found for column " # crit.key);
                            };
                            case (?index) {
                                return #err("Isnull not implemented for unique indexes");
                            };
                        };
                    }
                    else if(col.sortable) {
                        switch(nullMultIndexes.get(crit.key)) {
                            case null {
                                return #err("No index found for column " # crit.key);
                            };
                            case (?indexIds) {
                                set := indexIds;
                            };
                        };
                    };
                };
                case (val) {
                    if(col.unique) {
                        switch(uniqIndexes.get(crit.key)) {
                            case null {
                                return #err("No index found for column " # crit.key);
                            };
                            case (?index) {
                                switch(index.get(val)) {
                                    case null {
                                        return #ok(Set.empty<Id>());
                                    };
                                    case (?_id) {
                                        set := Set.put<Id>(set, _id, _hashId(_id), _equalId);
                                    };
                                };
                            };
                        };
                    }
                    else if(col.sortable) {
                        switch(multIndexes.get(crit.key)) {
                            case null {
                                return #err("No index found for column " # crit.key);
                            };
                            case (?index) {
                                switch(index.get(val)) {
                                    case null {
                                        return #ok(Set.empty<Id>());
                                    };
                                    case (?indexIds) {
                                        set := indexIds;
                                    };
                                };
                            };
                        };
                    };
                };
            };        

            if(Set.size<Id>(ids) == 0) {
                return #ok(set);
            }
            else {
                return #ok(Set.intersect<Id>(ids, set, _equalId));
            };                
        };

        func _filterByOther(
            crit: Criteria,
            ids: Set.Set<Id>,
            col: Column
        ): Result.Result<Set.Set<Id>, Text> {
            if(not (col.unique or col.sortable)) {
                return #err("No index found for column " # crit.key);
            };

            var set = Set.empty<Id>();

            switch(crit.value) {
                case (#nil) {
                    return #err("Operator doesn't support #nil");
                };
                case (val) {
                    if(col.unique) {
                        switch(uniqIndexes.get(crit.key)) {
                            case null {
                                return #err("No index found for column " # crit.key);
                            };
                            case (?index) {
                                let _ids = switch(crit.op) {
                                    case (#neq) index.findNeq(val);
                                    case (#lt) index.findLt(val);
                                    case (#lte) index.findLte(val);
                                    case (#gt) index.findGt(val);
                                    case (#gte) index.findGte(val);
                                    case (#between) {
                                        switch(val) {
                                            case (#tuple(a, b)) {
                                                index.findBetween(a, b);
                                            };
                                            case _ {
                                                return #err("Value should be a tuple for column " # crit.key);
                                            };
                                        };
                                    };
                                    case _ [];
                                };
                                
                                if(_ids.size() == 0) {
                                    return #ok(Set.empty<Id>());
                                }
                                else {
                                    set := Set.fromArray<Id>(
                                        Array.map(
                                            Array.filter(
                                                _ids, 
                                                func (_id: ?Id): Bool = Option.isSome(_id)
                                            ), 
                                            func (_id: ?Id): Id = 
                                                switch(_id) {
                                                    case null 0;
                                                    case (?_id) _id;
                                                },
                                        ),
                                        _hashId, 
                                        _equalId
                                    );
                                };
                            };
                        };
                    }
                    else if(col.sortable) {
                        switch(multIndexes.get(crit.key)) {
                            case null {
                                return #err("No index found for column " # crit.key);
                            };
                            case (?index) {
                                let _sets = switch(crit.op) {
                                    case (#neq) index.findNeq(val);
                                    case (#lt) index.findLt(val);
                                    case (#lte) index.findLte(val);
                                    case (#gt) index.findGt(val);
                                    case (#gte) index.findGte(val);
                                    case (#between) {
                                        switch(val) {
                                            case (#tuple(a, b)) {
                                                index.findBetween(a, b);
                                            };
                                            case _ {
                                                return #err("Value should be a tuple for column " # crit.key);
                                            };
                                        };
                                    };
                                    case _ [];
                                };

                                if(_sets.size() == 0) {
                                    return #ok(Set.empty<Id>());
                                }
                                else {
                                    for(_set in _sets.vals()) {
                                        switch(_set) {
                                            case (?_set) {
                                                set := Set.union<Id>(
                                                    set, 
                                                    _set,
                                                    _equalId
                                                );
                                            };
                                            case null 
                                            {
                                            };
                                        };
                                    };
                                };
                            };
                        };
                    };
                };
            };        

            if(Set.size<Id>(ids) == 0) {
                return #ok(set);
            }
            else {
                return #ok(Set.intersect<Id>(ids, set, _equalId));
            };                
        };

        func _filterByContains(
            crit: Criteria,
            ids: Set.Set<Id>
        ): Result.Result<Set.Set<Id>, Text> {
            switch(crit.value) {
                case (#nil) {
                    return #err("Value can't be null on column " # crit.key);
                };
                case (#text(val)) {
                    switch(partIndexes.get(crit.key)) {
                        case null {
                            return #err("No index found for column " # crit.key);
                        };
                        case (?index) {
                            let value = Utils.toLower(val);
                            switch(index.find(val)) {
                                case null {
                                    return #ok(Set.empty<Id>());
                                };
                                case (?set) {
                                    if(Set.size<Id>(ids) == 0) {
                                        return #ok(set);
                                    };
                                    
                                    return #ok(Set.intersect<Id>(ids, set, _equalId));
                                };
                            };
                        };
                    };
                };
                case _ {
                    return #err("Invalid type for column " # crit.key);
                };
            };
        };

        func _filterByStartsWith(
            crit: Criteria,
            ids: Set.Set<Id>
        ): Result.Result<Set.Set<Id>, Text> {
            switch(crit.value) {
                case (#nil) {
                    return #err("Value can't be null on column " # crit.key);
                };
                case (#text(val)) {
                    switch(prefixIndexes.get(crit.key)) {
                        case null {
                            return #err("No index found for column " # crit.key);
                        };
                        case (?index) {
                            let value = Utils.toLower(val);
                            switch(index.get(val)) {
                                case null {
                                    return #ok(Set.empty<Id>());
                                };
                                case (?set) {
                                    if(Set.size<Id>(ids) == 0) {
                                        return #ok(set);
                                    };

                                    return #ok(Set.intersect<Id>(ids, set, _equalId));
                                };
                            };
                        };
                    };
                };
                case _ {
                    return #err("Invalid type for column " # crit.key);
                };
            };
        };

        ///
        public func find(
            criterias: ?[Criteria],
            sortBy: ?[SortBy<E>],
            limit: ?Limit//,
            //joins: ?[Join<E>]
        ): Result.Result<[E], Text> {
        
            let limitBy = switch(limit) {
                case null {
                    {
                        offset = 0;
                        size = rows.size();
                    }
                };
                case (?limit) {
                    let offset = Nat.min(limit.offset, rows.size());

                    let size = if
                        (offset + limit.size > rows.size()) 
                            Nat.sub(rows.size(), offset)
                        else 
                            limit.size;

                    {
                        offset = offset;
                        size = size;
                    };
                };
            };

            var ids: [Id] = [];
            
            switch(criterias) {
                case null {
                    switch(_findAll(sortBy, limitBy)) {
                        case (#err(msg)) {
                            return #err(msg);
                        };
                        case (#ok(res)) {
                            ids := Buffer.toArray(res);
                        };
                    };
                };
                case (?criterias) {
                    if(criterias.size() == 0) {
                        switch(_findAll(sortBy, limitBy)) {
                            case (#err(msg)) {
                                return #err(msg);
                            };
                            case (#ok(res)) {
                                ids := Buffer.toArray(res);
                            };
                        };
                    }
                    else {
                        var set = Set.empty<Id>();

                        label l for(crit in criterias.vals()) {
                            if(Text.equal(crit.key, "_id")) {
                                switch(_filterById(crit, set)) {
                                    case (#err(msg)) {
                                        return #err(msg);
                                    };
                                    case (#ok(res)) {
                                        set := res;
                                    };
                                };
                            }
                            // not filtering by _id..
                            else {
                                switch(columns.get(crit.key)) {
                                    case null {
                                        return #err("Unknown column " # crit.key);
                                    };
                                    case (?col) {
                                        switch(crit.op) {
                                            case (#eq) {
                                                switch(_filterByEq(crit, set, col)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                            case (#contains) {
                                                switch(_filterByContains(crit, set)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                            case (#startsWith) {
                                                switch(_filterByStartsWith(crit, set)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                            case _ {
                                                switch(_filterByOther(crit, set, col)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                        };
                                    };
                                };
                            };                    
                            
                            if(Set.size<Id>(set) == 0) {
                                break l;
                            };
                        };

                        ids := Set.toArray<Id>(set);
                    };
                };
            };
            
            if(ids.size() == 0) {
                return #ok([]);
            };

            //
            var res = Buffer.Buffer<E>(ids.size());
            for(_id in ids.vals()) {
                switch(rows.get(Nat32.toNat(_id) - 1)) {
                    case null {
                    };
                    case (?row) {
                        res.add(row);
                    };
                };
            };

            // joining
            /*switch(joins) {
                case null {
                };
                case (?joins) {
                    switch(_joinTables(res, joins)) {
                        case (#err(msg)) {
                            return #err(msg);
                        };
                        case (#ok(joined)) {
                            res := joined;
                        };
                    };
                };
            };*/

            // if there are no criterias, the rows are already sorted and sliced
            switch(criterias) {
                case null {
                    return #ok(Buffer.toArray(res));
                };
                case (?criterias) {
                    if(criterias.size() == 0) {
                        return #ok(Buffer.toArray(res));
                    };
                };
            };

            // sorting
            switch(sortBy) {
                case null {
                };
                case (?sortBy) {
                    var i = sortBy.size();
                    while(i > 0) {
                        let sort = sortBy[i - 1];
                        Sort.sort<E>(res, sort.cmp);
                        i -= 1;
                    };
                };
            };

            // limit
            switch(limit) {
                case null {
                };
                case _ {
                  if(limitBy.offset >= res.size()) {
                      return #ok([]);
                  };
                  
                  let size = Nat.sub(Nat.min(limitBy.offset + limitBy.size, res.size()), 1);
                  
                  let sliced = Buffer.Buffer<E>(Nat.sub(size, limitBy.offset) + 1);
                  for(i in Iter.range(limitBy.offset, size)) {
                      sliced.add(res.get(i));
                  };

                  res := sliced;
                };
            };

            return #ok(Buffer.toArray(res));
        };

        func _findAll(
            sortBy: ?[SortBy<E>],
            limit: Limit
        ): Result.Result<Buffer.Buffer<Id>, Text> {

            switch(sortBy) {
                case null {
                    return _findAllById(#asc, limit);
                };
                case (?sortBy) {
                    if(sortBy.size() == 0) {
                        return _findAllById(#asc, limit);
                    };

                    let sort = sortBy[0];
                    switch(sort.key) {
                        case "_id" {
                            return _findAllById(sort.dir, limit);
                        };
                        case (key) {
                            switch(columns.get(key)) {
                                case null {
                                    return #err("Unknown column " # key);
                                };
                                case (?col) {
                                    if(col.unique) {
                                        return _findAllUnique(sort, limit);
                                    }
                                    else if(col.sortable) {
                                        return _findAllMultiple(sort, limit);
                                    }
                                    else {
                                        return #err("No index found for column " # key);
                                    };
                                };
                            };
                        };
                    };
                };
            };
        };

        func _findAllById(
            dir: SortDirection,
            limit: Limit
        ): Result.Result<Buffer.Buffer<Id>, Text> {
            let ids = Buffer.Buffer<Id>(100);
            
            var i = limit.offset;
            let total = rows.size();
            var cnt = 0;
            let last = limit.offset + limit.size;

            switch(dir) {
                case (#asc) {
                    while(i < total and cnt < last) {
                        let _id = Nat32.fromNat(1 + i);
                        switch(rows.getOpt(Nat32.toNat(_id) - 1)) {
                            case null {
                            };
                            case (?row) {
                                switch(row) {
                                    case null {
                                    };
                                    case (?row) {
                                        ids.add(_id);
                                        cnt += 1;
                                    };
                                };
                            };
                        };
                        i += 1;
                    };
                };
                case (#desc) {
                    while(i < total and cnt < last) {
                        i += 1;
                        let _id = Nat32.fromNat(1 + total - i);
                        switch(rows.getOpt(Nat32.toNat(_id) - 1)) {
                            case null {
                            };
                            case (?row) {
                                switch(row) {
                                    case null {
                                    };
                                    case (?row) {
                                        ids.add(_id);
                                        cnt += 1;
                                    };
                                };
                            };
                        };
                    };
                };
            };

            return #ok(ids);
        };

        func _findAllUnique(
            sort: SortBy<E>,
            limit: Limit
        ): Result.Result<Buffer.Buffer<Id>, Text> {
            let ids = Buffer.Buffer<Id>(100);
            
            switch(uniqIndexes.get(sort.key)) {
                case null {
                    return #err("No index found for column " # sort.key);
                };
                case (?index) {
                    var i = Nat32.fromNat(0);
                    let offset = Nat32.fromNat(limit.offset);
                    let last = offset + Nat32.fromNat(limit.size);
                    switch(sort.dir) {
                        case (#asc) {
                            var leaf = ?index.getFirstLeaf();
                            label l0 while(i < last) {
                                switch(leaf) {
                                    case null {
                                        break l0;
                                    };
                                    case (?l) {
                                        if(i + l.elements.cnt > offset) {
                                            var e = l.elements.head;
                                            var j = i;

                                            label l1 while(j < offset) {
                                                switch(e) {
                                                    case null {
                                                        break l1;
                                                    };
                                                    case (?elm) {
                                                        e := elm.next;
                                                    };
                                                };
                                                j += 1;
                                            };

                                            label l2 while(j < last) {
                                                switch(e) {
                                                    case null {
                                                        break l2;
                                                    };
                                                    case (?elm) {
                                                        switch(elm.value) {
                                                            case null {
                                                                assert false; loop { };
                                                            };
                                                            case (?_id) {
                                                                ids.add(_id);
                                                            };
                                                        };
                                                        j += 1;
                                                        e := elm.next;
                                                    };
                                                };
                                            };
                                        };
                                        
                                        i += l.elements.cnt;
                                        leaf := l.next;
                                    };
                                };
                            };
                        };
                        case (#desc) {
                            var leaf = ?index.getLastLeaf();
                            label l0 while(i < last) {
                                switch(leaf) {
                                    case null {
                                        break l0;
                                    };
                                    case (?l) {
                                        if(i + l.elements.cnt > offset) {
                                            var e = l.elements.tail;
                                            var j = i;

                                            label l1 while(j < offset) {
                                                switch(e) {
                                                    case null {
                                                        break l1;
                                                    };
                                                    case (?elm) {
                                                        e := elm.prev;
                                                    };
                                                };
                                                j += 1;
                                            };

                                            label l2 while(j < last) {
                                                switch(e) {
                                                    case null {
                                                        break l2;
                                                    };
                                                    case (?elm) {
                                                        switch(elm.value) {
                                                            case null {
                                                                assert false; loop { };
                                                            };
                                                            case (?_id) {
                                                                ids.add(_id);
                                                            };
                                                        };
                                                        j += 1;
                                                        e := elm.prev;
                                                    };
                                                };
                                            };
                                        };
                                        
                                        i += l.elements.cnt;
                                        leaf := l.prev;
                                    };
                                };
                            };                          
                        };
                    };

                    return #ok(ids);
                };
            };
        };

        func _findAllMultiple(
            sort: SortBy<E>,
            limit: Limit
        ): Result.Result<Buffer.Buffer<Id>, Text> {
            let ids = Buffer.Buffer<Id>(100);
            
            switch(multIndexes.get(sort.key)) {
                case null {
                    return #err("No index found for column " # sort.key);
                };
                case (?index) {
                    var i = Nat32.fromNat(0);
                    let offset = Nat32.fromNat(limit.offset);
                    let last = offset + Nat32.fromNat(limit.size);
                    switch(sort.dir) {
                        case (#asc) {
                            var leaf = ?index.getFirstLeaf();
                            label l1 while(i < last) {
                                switch(leaf) {
                                    case null {
                                        break l1;
                                    };
                                    case (?l) {
                                        var e = l.elements.head;
                                        label l2 while(i < last) {
                                            switch(e) {
                                                case null {
                                                    break l2;
                                                };
                                                case (?elm) {
                                                    switch(elm.value) {
                                                        case null {
                                                            assert false; loop { };
                                                        };
                                                        case (?set) {
                                                            let len = Nat32.fromNat(
                                                                Set.size<Id>(set));
                                                            if(i + len > offset) {
                                                                let arr = Array.sort<Id>(
                                                                    Set.toArray<Id>(set),
                                                                    _cmpIdOrder);
                                                                
                                                                var j = Nat32.toNat(
                                                                    if(i < offset) 
                                                                        offset - i
                                                                    else
                                                                        0);
                                                                let elms = Nat32.toNat(
                                                                    Nat32.min(len, last - i));

                                                                while(j < elms) {
                                                                    let _id = arr[j];
                                                                    ids.add(_id);
                                                                    j += 1;
                                                                };
                                                            };

                                                            i += len;
                                                            e := elm.next;
                                                        };
                                                    };
                                                };
                                            };
                                        };
                                        leaf := l.next;
                                    };
                                };
                            };
                        };
                        case (#desc) {
                            var leaf = ?index.getLastLeaf();
                            label l1 while(i < last) {
                                switch(leaf) {
                                    case null {
                                        break l1;
                                    };
                                    case (?l) {
                                        var e = l.elements.tail;
                                        label l2 while(i < last) {
                                            switch(e) {
                                                case null {
                                                    break l2;
                                                };
                                                case (?elm) {
                                                    switch(elm.value) {
                                                        case null {
                                                            assert false; loop { };
                                                        };
                                                        case (?set) {
                                                            let len = Nat32.fromNat(
                                                                Set.size<Id>(set));
                                                            if(i + len > offset) {
                                                                let arr = Array.sort<Id>(
                                                                    Set.toArray<Id>(set),
                                                                    _cmpIdReverseOrder);
                                                                
                                                                var j = Nat32.toNat(
                                                                    if(i < offset) 
                                                                        offset - i
                                                                    else
                                                                        0);
                                                                let elms = Nat32.toNat(
                                                                    Nat32.min(len, last - i));

                                                                while(j < elms) {
                                                                    let _id = arr[j];
                                                                    ids.add(_id);
                                                                    j += 1;
                                                                };
                                                            };

                                                            i += len;
                                                            e := elm.prev;
                                                        };
                                                    };
                                                };
                                            };
                                        };
                                        leaf := l.prev;
                                    };
                                };
                            };                           
                        };
                    };

                    return #ok(ids);
                };
            };        
        };

        /*func _resolveValueRef(
            value: Variant.Variant,
            entity: E
        ): Variant.Variant {
            switch(value) {
                case (#text(val)) {
                    if(not Text.startsWith(val, #text("$ref:"))) {
                        return value;
                    };
                    
                    let column = Text.trimStart(val, #text("$ref:"));
                    return getValue(column, entity);
                };
                case _ {
                    return value;
                };
            };
        };*/

        /*func _joinTables(
            rows: Buffer.Buffer<E>,
            joins: [Join<E>]
        ): Result.Result<Buffer.Buffer<E>, Text> {
            let res = Buffer.Buffer<E>(rows.size());

            for(row in rows.vals()) {
                let joined = Buffer.Buffer<E>(1);
                for(join in joins.vals()) {
                    let criterias = Array.map<Criteria, Criteria>(
                        join.criterias, 
                        func (c: Criteria): Criteria {
                            {
                                key = c.key;
                                op = c.op;
                                value = _resolveValueRef(c.value, row);
                            }
                        });

                    switch(join.table.find(?criterias, null, null, null)) {
                        case (#err(msg)) {
                            return #err(msg);
                        };
                        case (#ok(others)) {
                            var i = 0;
                            for(other in others.vals()) {
                                if(i == joined.size()) {
                                    joined.add(getJoined(row, join.prefix, other));
                                }
                                else {
                                    joined.put(i, getJoined(joined.get(i), join.prefix, other));
                                };
                                i += 1;
                            };
                        };
                    };
                };

                for(join in joined.vals()) {
                    res.add(join);
                };
            };

            return #ok(res);
        };*/

        ///
        public func count(
            criterias: ?[Criteria]
        ): Result.Result<Nat, Text> {
        
            var ids: [Id] = [];
            let limit = {
                offset = 0; 
                size = rows.size();
            };
            
            switch(criterias) {
                case null {
                    switch(_findAllById(#asc, limit)) {
                        case (#err(msg)) {
                            return #err(msg);
                        };
                        case (#ok(res)) {
                            ids := Buffer.toArray(res);
                        };
                    };
                };
                case (?criterias) {
                    if(criterias.size() == 0) {
                        switch(_findAllById(#asc, limit)) {
                            case (#err(msg)) {
                                return #err(msg);
                            };
                            case (#ok(res)) {
                                ids := Buffer.toArray(res);
                            };
                        };
                    }
                    else {
                        var set = Set.empty<Id>();

                        label l for(crit in criterias.vals()) {
                            if(Text.equal(crit.key, "_id")) {
                                switch(_filterById(crit, set)) {
                                    case (#err(msg)) {
                                        return #err(msg);
                                    };
                                    case (#ok(res)) {
                                        set := res;
                                    };
                                };
                            }
                            // not filtering by _id..
                            else {
                                switch(columns.get(crit.key)) {
                                    case null {
                                        return #err("Unknown column " # crit.key);
                                    };
                                    case (?col) {
                                        switch(crit.op) {
                                            case (#eq) {
                                                switch(_filterByEq(crit, set, col)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                            case (#contains) {
                                                switch(_filterByContains(crit, set)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                            case (#startsWith) {
                                                switch(_filterByStartsWith(crit, set)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                            case _ {
                                                switch(_filterByOther(crit, set, col)) {
                                                    case (#err(msg)) {
                                                        return #err(msg);
                                                    };
                                                    case (#ok(res)) {
                                                        set := res;
                                                    };
                                                };
                                            };
                                        };
                                    };
                                };
                            };                    
                            
                            if(Set.size<Id>(set) == 0) {
                                break l;
                            };
                        };

                        ids := Set.toArray<Id>(set);
                    };
                };
            };
            
            if(ids.size() == 0) {
                return #ok(0);
            };

            //
            var cnt = 0;
            for(_id in ids.vals()) {
                switch(rows.get(Nat32.toNat(_id) - 1)) {
                    case null {
                    };
                    case _ {
                        cnt += 1;
                    };
                };
            };

            return #ok(cnt);
        };

        func _insertIntoIndex(
            _id: Id,
            keys: [Variant.Variant],
            column: Text,
            props: Column
        ) {
            if(props.unique) {
                switch(uniqIndexes.get(column)) {
                    case null {
                        assert false; loop { };
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            index.put(key, ?_id);
                        };
                    };
                };
            };

            if(props.sortable) {
                switch(multIndexes.get(column)) {
                    case null {
                        assert false; loop { };
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            let ids = switch(index.get(key)) {
                                case null {
                                    Set.empty<Id>();
                                };
                                case (?ids) {
                                    ids;
                                };
                            };

                            let set = Set.put<Id>(ids, _id, _hashId(_id), _equalId);
                            index.put(key, ?set);
                        };
                    };
                };
            };
            
            // is partial search allowed?
            if(props.partial) {
                switch(partIndexes.get(column)) {
                    case null {
                        assert false; loop { };
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            switch(key) {
                                case (#text(val)) {
                                    index.put(val, _id);
                                };
                                case _ {
                                };
                            };
                        };
                    };
                };
            };

            // is prefixed search allowed?
            if(props.prefixed) {
                switch(prefixIndexes.get(column)) {
                    case null {
                        assert false; loop { };
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            switch(key) {
                                case (#text(val)) {
                                    var pref = "";
                                    for(char in Text.toIter(val)) {
                                        pref := Text.concat(pref, Text.fromChar(char));
                                        
                                        let ids = switch(index.get(pref)) {
                                            case null {
                                                Set.empty<Id>();
                                            };
                                            case (?ids) {
                                                ids;
                                            };
                                        }; 
                                        
                                        let set = Set.put<Id>(ids, _id, _hashId(_id), _equalId);
                                        index.put(pref, set);
                                    };
                                };
                                case _ {
                                };
                            };
                        };
                    };
                };
            };
        };

        func _insertIntoIndexes(
            _id: Id,
            entity: E,
            map: HashMap.HashMap<Text, Variant.Variant>
        ) {

            // for each column..
            label l for((column, props) in columns.entries()) {
                // skip primary
                if(props.primary) {
                    continue l;
                };
                
                switch(map.get(column)) {
                    case null {
                    }; 
                    case (?val) {
                        switch(val) {
                            // is value null?
                            case (#nil) {
                                if(props.unique) {
                                    nullUniqIndexes.put(column, ?_id);
                                }
                                else if(props.sortable) {
                                    switch(nullMultIndexes.get(column)) {
                                        case null {
                                            assert false; loop { };
                                        };
                                        case (?ids) {
                                            let set = Set.put<Id>(ids, _id, _hashId(_id), _equalId);
                                            nullMultIndexes.put(column, set);
                                        };
                                    };
                                    
                                };
                            };
                            // array?
                            case (#array(keys)) {
                                _insertIntoIndex(_id, keys, column, props);
                            };
                            // not null or array..
                            case (key) {
                                _insertIntoIndex(_id, [key], column, props);
                            };
                        };
                    };
                };
            };
        };

        func _canInsert(
            entity: E,
            map: HashMap.HashMap<Text, Variant.Variant>
        ): Result.Result<(), Text> {

            // for each column..
            label l for((column, props) in columns.entries()) {
                // skip primary
                if(props.primary) {
                    continue l;
                };
                
                switch(map.get(column)) {
                    case null {
                    }; 
                    case (?val) {
                        switch(val) {
                            // is value null?
                            case (#nil) {
                                switch(props.nullable) {
                                    // can not be null?
                                    case false {
                                        return #err("Value can not be null at column " # column);
                                    };
                                    case _ {
                                        if(props.unique) {
                                            // any index defined?
                                            switch(nullUniqIndexes.get(column)) {
                                                case null {
                                                };
                                                case _ {
                                                    return #err("Duplicated unique key at column " # column);
                                                };
                                            };
                                        };
                                    };
                                };
                            };
                            // array?
                            case (#array(keys)) {
                                if(props.unique) {
                                    // any index defined?
                                    switch(uniqIndexes.get(column))
                                    {
                                        case null {
                                        };
                                        case (?index) {
                                            for(key in keys.vals()) {
                                                switch(index.get(key)) {
                                                    case null {
                                                    };
                                                    case _ {
                                                        return #err("Duplicated unique key at column " # column);
                                                    };
                                                };
                                            };
                                        };
                                    };
                                };
                            };
                            // not null or array..
                            case (key) {
                                if(props.unique) {
                                    // any index defined?
                                    switch(uniqIndexes.get(column))
                                    {
                                        case null {
                                        };
                                        case (?index) {
                                            switch(index.get(key)) {
                                                case null {
                                                };
                                                case _ {
                                                    return #err("Duplicated unique key at column " # column);
                                                };
                                            };
                                        };
                                    };
                                };
                            };
                        };
                    };
                };
            };

            return #ok();
        };

        func _deleteFromIndex(
            _id: Id,
            keys: [Variant.Variant],
            column: Text,
            props: Column
        ) {
            if(props.unique) {
                switch(uniqIndexes.get(column)) {
                    case null {
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            index.delete(key);
                        };
                    };
                };
            };

            if(props.sortable) {
                switch(multIndexes.get(column)) {
                    case null {
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            switch(index.get(key)) {
                                case null {
                                };
                                case (?ids) {
                                    let deleted = Set.delete<Id>(ids, _id, _hashId(_id), _equalId);
                                    if(Set.size<Id>(deleted) == 0) {
                                        index.delete(key);
                                    } 
                                    else {
                                        index.put(key, ?deleted);
                                    };
                                };
                            };
                        };
                    };
                };
            };

            // is partial search allowed?
            if(props.partial) {
                switch(partIndexes.get(column)) {
                    case null {
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            switch(key) {
                                case (#text(val)) {
                                    index.delete(val, _id);
                                };
                                case _ {
                                };
                            };
                        };
                    };
                };
            };

            // is prefixed search allowed?
            if(props.prefixed) {
                switch(prefixIndexes.get(column)) {
                    case null {
                    };
                    case (?index) {
                        for(key in keys.vals()) {
                            switch(key) {
                                case (#text(val)) {
                                    var pref = "";
                                    for(char in Text.toIter(val)) {
                                        pref := Text.concat(pref, Text.fromChar(char));

                                        switch(index.get(pref)) {
                                            case null {
                                            };
                                            case (?ids) {
                                                let deleted = Set.delete<Id>(ids, _id, _hashId(_id), _equalId);
                                                if(Set.size<Id>(deleted) == 0) {
                                                    index.delete(pref);
                                                } 
                                                else {
                                                    index.put(pref, deleted);
                                                };
                                            };
                                        };
                                    };
                                };
                                case _ {
                                };
                            };
                        };
                    };
                };
            };
        };

        func _deleteFromIndexes(
            _id: Id,
            entity: E,
            map: HashMap.HashMap<Text, Variant.Variant>
        ) {
            label l for((column, props) in columns.entries()) {
                // skip primary
                if(props.primary) {
                    continue l;
                };
                
                switch(map.get(column)) {
                    case null {
                    };
                    case (?val) {
                        switch(val) {
                            // is value null?
                            case (#nil) {
                                if(props.unique) {
                                    nullUniqIndexes.put(column, null);
                                }
                                else if(props.sortable) {
                                    switch(nullMultIndexes.get(column)) {
                                        case null {
                                            assert false; loop { };
                                        };
                                        case (?ids) {
                                            let deleted = Set.delete<Id>(ids, _id, _hashId(_id), _equalId);
                                            nullMultIndexes.put(column, deleted);
                                        };
                                    };
                                };
                            };
                            // array?
                            case(#array(keys)) {
                                _deleteFromIndex(_id, keys, column, props);
                            };
                            // not null or array..
                            case (key) {
                                _deleteFromIndex(_id, [key], column, props);
                            };
                        };
                    };
                };
            };
        };

		func _validate(
			map: HashMap.HashMap<Text, Variant.Variant>
		): Result.Result<(), [Text]> {
            var errors = Buffer.Buffer<Text>(1);
			
			// for each column..
            label l for((column, props) in columns.entries()) {
                switch(map.get(column)) {
                    case null {
						if(props.nullable == false) {
							errors.add(column # " can't be null");
						};
                    }; 
                    case (?val) {
                        switch(val) {
                            // is value null?
                            case (#nil) {
                                if(props.nullable == false) {
									errors.add(column # " can't be null");
								};
								if(props.min > 0) {
									errors.add(column # " must be at least " # Nat.toText(props.min));
								};
							};
							case (#text(val)) {
								if(val.size() < props.min) {
									errors.add(column # " must be at least " # Nat.toText(props.min) # " long");
								}
								else if(val.size() > props.max) {
									errors.add(column # " must be at most " # Nat.toText(props.max) # " long");
								};
							};
							case (#nat32(val)) {
								if(Nat32.toNat(val) < props.min) {
									errors.add(column # " must be at least " # Nat.toText(props.min));
								}
								else if(Nat32.toNat(val) > props.max) {
									errors.add(column # " must be at most " # Nat.toText(props.max));
								};
							};
							case (#nat64(val)) {
								if(Nat64.toNat(val) < props.min) {
									errors.add(column # " must be at least " # Nat.toText(props.min));
								}
								else if(Nat64.toNat(val) > props.max) {
									errors.add(column # " must be at most " # Nat.toText(props.max));
								};
							};
							case (#nat(val)) {
								if(val < props.min) {
									errors.add(column # " must be at least " # Nat.toText(props.min));
								}
								else if(val > props.max) {
									errors.add(column # " must be at most " # Nat.toText(props.max));
								};
							};
							case (#array(arr)) {
								if(arr.size() < props.min) {
									errors.add(column # " must have at least " # Nat.toText(props.min) # " elements");
								}
								else if(arr.size() > props.max) {
									errors.add(column # " must have at most " # Nat.toText(props.max) # " elements");
								};
							};
							case _ {
								if(props.min > 0) {
									errors.add("Min() validation not implemented for the type of column " # column);
								};
								if(props.max < 2**32) {
									errors.add("Max() validation not implemented for the type of column " # column);
								};
							};
						};
					};
				};
			};
			
			if(errors.size() == 0) {
				return #ok();
			}
			else {
				return #err(Buffer.toArray(errors));
			};
		};

        ///
        public func backup(
        ): [[(Text, Variant.Variant)]] {
            D.print("begin table('" # schema.name # "') backup: " # Nat.toText(rows.size()) # " rows");
            let entities = Buffer.Buffer<[(Text, Variant.Variant)]>(rows.size());
            for(row in rows.vals()) {
                switch(row) {
                    case null {
                    };
                    case (?row) {
                        entities.add(_entityToFields(row));
                    };
                };
            };
            let res = Buffer.toArray(entities);
            D.print("end table('" # schema.name # "') backup: " # Nat.toText(res.size()) # " entities");
            return res;
        };
		
		func _entityToFields(
            entity: E
        ): [(Text, Variant.Variant)] {
            let fields = Buffer.Buffer<(Text, Variant.Variant)>(10);

            let map = serialize(entity, false);

            for((key, val) in map.entries()) {
                fields.add(key, val);
            };

            return Buffer.toArray(fields);
        };

        func _fieldsToMap(
            value: [(Text, Variant.Variant)]
        ): HashMap.HashMap<Text, Variant.Variant> {
            let map = HashMap.HashMap<Text, Variant.Variant>(value.size(), Text.equal, Text.hash);
            for(entry in value.vals()) {
                map.put(entry.0, entry.1);
            };
            return map;
        };

        ///
        public func restore(
            entities: [[(Text, Variant.Variant)]]
        ) {
            D.print("begin table('" # schema.name # "') restore: " # Nat.toText(entities.size()) # " entities");
            var i = Nat32.fromNat(1);
            for(entry in entities.vals()) {
                let map = _fieldsToMap(entry);
                let _id = Variant.getOptNat32(map.get("_id"));
                while(_id > i) {
                    rows.add(null);
                    i += 1;
                };
                let entity = deserialize(map);
                rows.add(?entity);
                let mapLC = serialize(entity, true);
                _insertIntoIndexes(_id, entity, mapLC);
                i += 1;
            };
            D.print("end table('" # schema.name # "') restore: " # Nat.toText(rows.size()) # " rows");
        };
    };
};