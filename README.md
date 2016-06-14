# Duratom
<p align="center">
  <img src="https://pbs.twimg.com/profile_images/681519713005006848/HgkHYOWb_400x400.png"/>
</p>

A durable atom type for Clojure. Duratom implements the same interfaces as the core Clojure atom (IAtom, IRef, IDeref), plus the `IDurable` protocol from `duratom.core`.
In order to provide durability `duratom` will persist its state to some durable-backend on each mutation. The built-in backends are:
 
 1. A file on the local file-system
 2. A postgres DB row
 3. An AWS-S3 key 

Note: Several ideas taken/adapted/combined from [enduro](https://github.com/alandipert/enduro) & [durable-atom](https://github.com/polygloton/durable-atom)

Main difference between `duratom` & `enduro` is that an `enduro` atom is not a drop-in replacement for regular clojure atoms. In particular:
  
  1. it doesn't implement all the same interfaces as regular clojure atoms. As a result it comes with its own `swap!` & `reset!` implementations.
  2. it requires the watches/validators to be provided in atoms upon construction.  

Main difference between `duratom` & `durable-atom` is that a `durable-atom` atom doesn't have a second level of polymorphism to accommodate for switching storage backends. It assumes that a file-backed atom is always what you want. Moreover, it uses `slurp` & `spit` for reading/writing to the disk, which, in practice, puts a limit on how big data-structures you can fit in a String (depending on your hardware & JVM configuration of course). Finally, it uses `locking` which is problematic on some JVMs (e.g. certain IBM JVM versions). `duratom` uses the `java.util.concurrent.locks.Lock` interface instead.

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
         :db-config "any db-spec understood by clojure.java.jdbc"
         :table-name "my_table"
         :row-id 0
         :init {:x 1 :y 2}) 
                  
;; backed by S3
(duratom :aws-s3 
         :credentials "as understood by amazonica"
         :bucket "my_bucket"
         :key "0"
         :init {:x 1 :y 2})                   
```

The initial-value <init> is ignored, unless the underlying persistent storage is found to be empty.
If you prefer passing arguments positionally, you can use the `file-atom`, `postgres-atom` & `s3-atom` equivalents.

## Requirements
Clojure 1.8 

## License

Copyright © 2016 Dimitrios Piliouras

Distributed under the Eclipse Public License, the same as Clojure.
