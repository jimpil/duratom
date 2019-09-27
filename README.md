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
         :db-config "any db-spec as understood by clojure.java.jdbc"
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

The initial-value <init> can be a concrete value (as show above), but also a no-arg fn or a delay. In any case, it may end up being completely ignored (i.e. if the underlying persistent storage is found to be non-empty).
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

## Asynchronous commits (by default)
In `duratom` persisting to storage happens asynchronously (via an `agent`). This ensures minimum overhead  (duratoms feel like regular atoms regardless of the storage backend), but more importantly safety (writes never collide). However, this also means that if you *manually* take a peek at storage without allowing sufficient time for the writes, you might momentarily see inconsistent values between the duratom and its storage. That is not a problem though, it just means that the state of the duratom won't necessarily be the same as the persisted state at *all* times. For instance, this is precisely why you will find some `Thread/sleep` expressions in the `core_test.clj` namespace. 

If you're not comfortable with the above, or if for whatever reason you prefer synchronous commits, `duratom 0.4.3` adds support for them. You just need to provide some value as the `:commit-mode` in your `rw` map. That value can be anything but nil, nor `:duratom.core/async` (I use `:sync` in the unit-tests) as these two are reserved for async commits. Consequently, existing users that were relying on custom readers/writers are not affected.  
   


## Default EDN readers
As of version `0.4.2`, `duratom` makes an effort (by default) to support certain (important from an `atom` usage perspective) collections, that are not part of the EDN spec. These are the two built-in sorted collections (map/set), and the somewhat hidden, but otherwise very useful `clojure.lang.PesistentQueue`. Therefore, for these particular collections you can expect correct EDN round-tripping (printing/reading), without losing the type along the way. It does this, by outputting custom tags (i.e. `#sorted/map`, `#sorted/set` \& `#queue`), and then reading those back with custom `:readers` (via `clojure.edn/read`). More details can be seen in the `duratom.readers.clj` namespace. If you don't like the default ones, feel free to provide your own, but keep in mind that you need to do it at both ends (reading AND printing via `print-method`). Again, `duratom.readers.clj` showcases how to do this. The same trick can be played with other cool collections (e.g. `clojure.data.priority-map`).

This can perhaps be viewed as a breaking change, albeit one that probably won't break any existing programs. If you've previously serialised a sorted-map with `duratom`, you've basically lost the type. Reading it back with 0.4.2, will result in plain map, the same as with any other version (the type is lost forever). Therefore, as far as I can see, no existing programs should break by upgrading to 0.4.2.

## Metadata
Similarly to the aforementioned important types, as of version `0.4.2`, `duratom` also makes an effort (by default) to preserve metadata. It does this by wrapping the collection provided in a special type (constructed via `utils/iobj->edn-tag`), and prints that instead, emitting a special tag `#duratom/iobj`. Then at the other end (reading), a custom EDN reader is provided specifically for this tag. This only happens for collections that have non-nil metadata.

Even though this sounds like a major breaking-change, it actually isn't! Similar to the sorted-map example earlier, if you've previously serialised a map (with metadata) with `duratom`, you've lost that metadata. Reading it back with 0.4.2, will result in a  map without metadata, the same as with any other version (the metadata is lost forever). If you then `swap!` with a fn which adds metadata, then that will be custom-printed (with the new custom tag), and read just fine later. As far as I can see existing programs (using the default readers/writers) should not break by upgrading to 0.4.2. Projects that use their own EDN reading/printing, can of course merge their readers (if any) with `duratom.readers/default`. If you have a breaking use-case, please report it asap. It will be much appreciated.  

If you are perfectly content with losing metadata and want to revert to the previous default behaviour, you can do so by overriding the default writer (given your backend). For example, replace `ut/write-edn-object` with `(partial ut/write-edn-object false)` as the default writer, and the newly added metadata support will be completely sidestepped (i.e. the `#duratom/iobj` tag will never be emitted).

## Requirements

- Java >= 8
- Clojure >= 1.10

### Optional Requirements

- [clojure.java.jdbc](https://github.com/clojure/java.jdbc) >= 0.6.0
- [amazonica](https://github.com/mcohen01/amazonica)

## Development

Start databases with a command `docker-compose up` before running the tests

## License

Copyright © 2016 Dimitrios Piliouras

Distributed under the Eclipse Public License, the same as Clojure.
