# Duratom

A durable atom type for Clojure. Duratom implements the same interfaces as the core Clojure atom (IAtom, IRef, IDeref), plus the `IDurable` protocol from `duratom.core`.
In order to provide durability `duratom` will persist its state to some durable-backend on each mutation. The built-in backends are:
 
 1. A file on the local file-system
 2. A postgres DB instance

Note: Several ideas taken/adapted from [enduro](https://github.com/alandipert/enduro) 

## Usage
The public API consists of a single constructor function (`duratom.core/duratom`). Once you have constructed a duratom object, you can use it just like a regular atom,
with the slight addition that when you're done with it, you can call `duratom.core/destroy` on it to clear the durable backend (e.g. delete the file/table). 
Subsequent mutating operations are prohibited (only `deref`ing will work).

###Example

```clj

;; backed by file
(duratom :local-file 
         :file-path "/home/dimitris/Desktop/data.txt"
         :init {:x 1 :y 2})
         
;; backed by postgres-db
(duratom :postgres-db 
         :db-config "any-db-spec-understood-by-clojure.java.jdbc"
         :table-name "my_table"
         :init {:x 1 :y 2})          
```

The initial-value <init> is ignored, unless the underlying persistent storage is found to be empty.
If you prefer passing arguments positionally, you can use the `file-atom` & `postgres-atom` equivalents.

## Requirements
Clojure 1.8 

## License

Copyright © 2016 Dimitrios Piliouras

Distributed under the Eclipse Public License, the same as Clojure.
