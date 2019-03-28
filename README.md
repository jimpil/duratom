# Duratom
<p align="center">
  <img src="https://pbs.twimg.com/profile_images/681519713005006848/HgkHYOWb_400x400.png"/>
</p>

## Where

[![Clojars Project](https://clojars.org/duratom/latest-version.svg)](https://clojars.org/duratom)

## What

A durable atom type for Clojure. Duratom implements the same interfaces as the core Clojure atom (IAtom, IRef, IDeref).
In order to provide durability `duratom` will persist its state to some durable-backend on each mutation. The built-in backends are:

 1. A file on the local file-system
 2. A postgres DB table row
 3. An AWS-S3 bucket key

Note: Several ideas taken/adapted/combined from [enduro](https://github.com/alandipert/enduro) & [durable-atom](https://github.com/polygloton/durable-atom)

Main difference between `duratom` & `enduro` is that an `enduro` atom is not a drop-in replacement for regular clojure atoms. In particular:

  1. it doesn't implement all the same interfaces as regular clojure atoms. As a result it comes with its own `swap!` & `reset!` implementations.
  2. it requires the watches/validators to be provided in atoms upon construction.

Main difference between `duratom` & `durable-atom` is that a `durable-atom` atom doesn't have a second level of polymorphism to accommodate for switching storage backends. It assumes that a file-backed atom is always what you want. Moreover, it uses `slurp` & `spit` for reading/writing to the disk, which, in practice, puts a limit on how big data-structures you can fit in a String (depending on your hardware & JVM configuration of course). Finally, it uses `locking` which is problematic on some JVMs (e.g. certain IBM JVM versions). `duratom` uses the `java.util.concurrent.locks.Lock` interface instead.

## Usage

The public API consists of a single constructor function (`duratom.core/duratom`). Once you have constructed a duratom object, you can use it just like a regular atom, with the slight addition that when you're done with it, you can call `duratom.core/destroy` on it to clear the durable backend (e.g. delete the file/table).

Subsequent mutating operations are prohibited (only `deref`ing will work).

### Example

```clj

;; backed by file
(duratom :local-file
         :file-path "/home/..."
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

## Custom :read & :write

By default duratom stores plain EDN data (via `pr-str`). If that's good enough for your use too, you can skip this section. Otherwise, observe the following examples of utilising the `:rw` keyword for specifying [nippy](https://github.com/ptaoussanis/nippy) as the encoder/decoder of that EDN data:

```clj
;; nippy encoded bytes on the filesystem 
(duratom :local-file
         :file-path "/home/..."
         :init {:x 1 :y 2}
         :rw {:read nippy/thaw-from-file
              :write nippy/freeze-to-file})

;; nippy encoded bytes on PostgresDB
(duratom :postgres-db
         :db-config "any db-spec understood by clojure.java.jdbc"
         :table-name "my_table"
         :row-id 0
         :init {:x 1 :y 2}
         :rw {:read nippy/thaw
              :write nippy/freeze
              :column-type :bytea})
          
;;nippy encoded bytes on S3 
(duratom :aws-s3
         :credentials "as understood by amazonica"
         :bucket "my_bucket"
         :key "0"
         :init {:x 1 :y 2}
         :rw {:read (comp nippy/thaw utils/s3-bucket-bytes)
              :write nippy/freeze})          

```

## Preserving types
As of version `0.4.2`, `duratom` makes an effort (by default) to support certain (important from an `atom` perspective) collections,
 that are not part of the EDN spec. These are the two built-in sorted collections (map/set), and the somewhat hidden, but otherwise
  very useful `clojure.lang.PesistentQueue.java`. Therefore, for these particular collections you can expect correct round-tripping
 (printing/reading), without losing the type along the way. It does this, by outputting custom tags (i.e. `#sorted/map`, `#sorted/set`
  \& `#queue`), and then reading those back with custom `:readers` (via `clojure.edn/read`). More details can be seen in the 
  `duratom.readers.clj` namespace. If you don't like the default ones, feel free to provide your own, but keep in mind that you 
  need to do it at both ends (reading AND printing). Again, `duratom.readers.clj` showcases how to do this.

## Metadata
Metadata, as conveyed and understood by Clojure, are not part of the EDN spec. Therefore one cannot expect them to be preserved during EDN (de)-serialisation. However, the metadata itself is just another map, and therefore this is not really an issue. If you want this to happen somewhat 'auto-magically', `duratom` includes an object (constructed via `utils/iobj->edn-tag`) which can be used to wrap your collection before passing it for printing. This object is nothing special - in fact it does absolutely nothing! It's a placeholder object which `duratom` prints unpacked into a vector tuple with a special tag `#iobj` - and reads it back with a custom reader which simply packs it back. It does sound rather convenient, and it will work (see `readers_test.clj`), however I would still not recommend actually doing that. It feels way too magical and somewhat indirect. It is completely trivial to do this process manually before `duratom` ever sees your data. Simply unpack the collection into a vector of two elements (the coll and its metadata-map), and pass that to `duratom`. Then at the other end, turning that vector into a collection with some metadata is just a matter of calling `apply with-meta` on it. Despite practically equivalent from a runtime POV, the latter approach will look much more evident (from a source-code POV). The choice is yours ;) 


## Requirements

Java >= 8
Clojure >= 1.9

### Optional Requirements

- [clojure.java.jdbc](https://github.com/clojure/java.jdbc) >= 0.6.0
- [amazonica](https://github.com/mcohen01/amazonica)

## License

Copyright © 2016 Dimitrios Piliouras

Distributed under the Eclipse Public License, the same as Clojure.
