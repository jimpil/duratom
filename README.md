# Duratom
<p align="center">
  <img src="https://pbs.twimg.com/profile_images/681519713005006848/HgkHYOWb_400x400.png"/>
</p>

## Where

[![Clojars Project](https://clojars.org/duratom/latest-version.svg)](https://clojars.org/duratom)

## What

A durable atom/agent type for Clojure. `duratom` implements the same interfaces as the core Clojure atom (IAtom, IRef, IDeref), 
whereas `duragent` **is** a Clojure agent (with a special watch + some metadata). 
In order to provide durability `duratom`/`duragent` will persist its state to some durable-backend on each mutation. The built-in backends are:

 1. A file on the local file-system
 2. A postgres DB table row
 3. A SQLite DB table row
 4. An AWS-S3 bucket key
 5. A Redis DB key (*)
 6. [file.io](https://www.file.io/)(**)

Note: Several ideas taken/adapted/combined from [enduro](https://github.com/alandipert/enduro) & [durable-atom](https://github.com/polygloton/durable-atom)

Main difference between `duratom` & `enduro` is that an `enduro` atom is not a drop-in replacement for regular clojure atoms. In particular:

  1. it doesn't implement all the same interfaces as regular clojure atoms. As a result it comes with its own `swap!` & `reset!` implementations.
  2. it requires the watches/validators to be provided in atoms upon construction.

Main difference between `duratom` & `durable-atom` is that a `durable-atom` atom doesn't have a second level of polymorphism to accommodate for switching storage backends. It assumes that a file-backed atom is always what you want. Moreover, it uses `slurp` & `spit` for reading/writing to the disk, which, in practice, puts a limit on how big data-structures you can fit in a String (depending on your hardware & JVM configuration of course). Finally, it uses `locking` which is problematic on some JVMs (e.g. certain IBM JVM versions). `duratom` uses the `java.util.concurrent.locks.Lock` interface instead.

(*) Redis is an in-memory data structure store with optional [persistence](https://redis.io/topics/persistence). It might not be the best option in those cases where you absolutely cannot lose the state backed by `duratom`. But if you can, it is a fast, flexible and lightweight backend option for the durable atom. Moreover, it's worth noting that the value in the atom (wrapped by duratom) will exist in two places in memory (regular atom + Redis).

(**) `file.io` is an interesting service. Not only is it free, but also effectively unlimited (when not abused). 
Yes, there are practical limits with respect to file-sizes and rate-limits on the (public) API, but in certain use cases these can be
mitigated. For instance, a duratom (or a duragent for that matter) never reads from storage after it's been initialised - it only writes. Therefore, in a low-write situation (which is the ideal use-case anyway) the API rate-limiting just isn't a concern. 
Sizes if up to 5GB(!) are allowed, but since we're sending a String over, the effective limit is actually around 2GB.
The tricky thing is to manage the ephemeral nature of the service, and to avoid abusing it. In order to achieve this, and maintain
persistence guarrantees, two things are required. The backend object itself must be clever enough to restore the state on 
every read (as discussed there is only one of those), and to delete previous states on every new commit. 
The caller must provide a `key-duratom` that will hold the unique ID of each new commit (a short String).
 If the system crashes, that `key-duratom` will allow the fileio-duratom to restore its state. This does make things 
 slightly more complicated, but since the key is fundamentally dynamic (changes on each commit), there is no other way around it.
 Finally, the caller must provide a `:http-post!` fn of two arguments (URL and a String) - see below in the example section for 
 the specifics, and a good candidate fn that should work with both [clj-http](https://github.com/dakrone/clj-http) and 
 [http-kit](https://www.http-kit.org/client.html).     
  



## Usage

The public API consists of two constructor function (`duratom.core/duratom` and `duratom.core/duragent`). The same parameters
are applicable to both apart from `:commit-mode`, which is a no-op for `duragent`. 
Once you have constructed a duratom object, you can use it just like a regular atom, with the slight addition that when 
you're done with it, you can call `duratom.core/destroy` on it to clear the durable backend (e.g. delete the file/table).
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

;; backed by sqlite-db
(duratom :sqlite-db
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

;; backed by Redis
(duratom :redis-db
         :db-config "any db-spec as understood by carmine"
         :key-name "my:key"
         :init {:x 1 :y 2})

;; backed by file.io

;; firstly we need a function to do the POST-ing
(defn http-post
  [url ^String data]
  @(http/post url ;; assuming `http-kit`
     {:form-params {:text data}
      :headers {"Content-Type" "application/x-www-form-urlencoded"}}))

;; secondly we need a duratom to hold the (ever-changing) key
(def key-duratom 
  (duratom :local-file :file-path "/tmp/fio.key")) ;; no init!

;; we are now ready to create the final duratom
(duratom :file.io
         :http-post!  http-post
         :key-duratom key-duratom 
         :init {:x 1 :y 2})

;; finally it's worth noting that the file.io backend expects text. In other words, `nippy` might not work here.
```

The initial-value <init> can be a concrete value (as shown above), but also a no-arg fn, or a `delay`. In any case, it may end up being completely ignored (i.e. if the underlying persistent storage is found to be non-empty).
If you prefer passing arguments positionally, you can use the `file-atom`, `postgres-atom`, `s3-atom`, `redis-atom` 
and `fileio-atom` equivalents.

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

;;Carmine uses Nippy under the hood for Redis when Clojure types are passed in directly
(duratom :redis-db
         :db-config "any db-spec as understood by carmine"
         :key-name "my:key"
         :init {:x 1 :y 2}
         :rw {:read identity
              :write identity})
```

## Asynchronous commits (by default)
In `duratom` persisting to storage happens asynchronously (via an `agent`). This ensures minimum overhead  (duratoms feel like regular atoms regardless of the storage backend), but more importantly safety (writes never collide). However, this also means that if you *manually* take a peek at storage without allowing sufficient time for the writes, you might momentarily see inconsistent values between the duratom and its storage. That is not a problem though, it just means that the state of the duratom won't necessarily be the same as the persisted state at *all* times. For instance, this is precisely why you will find some `Thread/sleep` expressions in the `core_test.clj` namespace. 

If you're not comfortable with the above, or if for whatever reason you prefer synchronous commits, `duratom 0.4.3` adds support for them. You just need to provide some value as the `:commit-mode` in your `rw` map. That value can be anything but nil, nor `:duratom.core/async` (I use `:sync` in the unit-tests) as these two are reserved for async commits. Consequently, existing users that were relying on custom readers/writers are not affected.  
   

## Custom error-handling 

In order to fully understand error-handling, the distinction between a regular commit VS a re-commit, must be crystal clear. The former is attempted once by the construct itself (`duratom`/`duragent`), whereas the latter is (potentially) caller initiated (see below), and even possibly more than once. Whether or not it is safe to re-commit depends on the actual construct, and hopefully the caveats of certain situations should become apparent by the end of this section. 

`duratom` 0.4.7 adds support for user-defined error handling (when persisting a value fails), applicable to both synchronous and asynchronous commits (see previous section) The `:rw` map, can now take an `:error-handler` key, which for the duratom case should be pointing to a function of 2 arguments - the exception object thrown, and a no-arg fn which when called will retry the commit call (recommit). For the duragent case that fn should have two arities - the usual `[agent exception]` arity which will handle errors unrelated to persistence, and `[agent exception recommit-fn]` which will handle all the persistence errors. You can use this facility to (at least) log the error thrown - whether (or not) it's worth recommitting depends entirely on the application (e.g. how critical it is for a particular commit to succeed). Moreover, it's worth noting that exceptions thrown from within the error-handler will be swallowed. This is natural behaviour for an agent (which is relied on for async commits), and is mimicked in the synchronous scenario too.  If `:error-handler` is not provided, defaults to `(constantly nil)`. 

### Recommitting

As established in the previous section, by default a `duratom` will dispatch commits of the new state (of the underlying atom) via an internal agent. This setup gains async commit semantics, but doesn't play nicely with re-committing in the provided error-handler (per the previous paragraph), simply because the atom's state might have changed by the time the recommit fires. A synchronous `duratom` doesn't suffer from this symptom, as the entire commit call stays behind a lock (as opposed to `send-off` which escapes the scope of the lock). Neither does a `duragent`, because the recommit will happen on the agent's dispatch thread before any pending sends (i.e. synchronously wrt to the original commit).

#### TL:DR 
If you use `duratom`, and you also want to be able to safely recommit from within the error-handler, consider using it in synchronous commit-mode, or switch to `duragent`.


## Default EDN readers
As of version `0.4.2`, `duratom` makes an effort (by default) to support certain (important from an `atom` usage perspective) collections, that are not part of the EDN spec. These are the two built-in sorted collections (map/set), and the somewhat hidden, but otherwise very useful `clojure.lang.PesistentQueue`. Therefore, for these particular collections you can expect correct EDN round-tripping (printing/reading), without losing the type along the way. It does this, by outputting custom tags (i.e. `#sorted/map`, `#sorted/set` \& `#queue`), and then reading those back with custom `:readers` (via `clojure.edn/read`). More details can be seen in the `duratom.readers.clj` namespace. If you don't like the default ones, feel free to provide your own, but keep in mind that you need to do it at both ends (reading AND printing via `print-method`). Again, `duratom.readers.clj` showcases how to do this. The same trick can be played with other cool collections (e.g. `clojure.data.priority-map`).

This can perhaps be viewed as a breaking change, albeit one that probably won't break any existing programs. If you've previously serialised a sorted-map with `duratom`, you've basically lost the type. Reading it back with 0.4.2, will result in plain map, the same as with any other version (the type is lost forever). Therefore, as far as I can see, no existing programs should break by upgrading to 0.4.2.

## Metadata
Similarly to the aforementioned important types, as of version `0.4.2`, `duratom` also makes an effort (by default) to preserve metadata. It does this by wrapping the collection provided in a special type (constructed via `utils/iobj->edn-tag`), and prints that instead, emitting a special tag `#duratom/iobj`. Then at the other end (reading), a custom EDN reader is provided specifically for this tag. This only happens for collections that have non-nil metadata.

Even though this sounds like a major breaking-change, it actually isn't! Similar to the sorted-map example earlier, if you've previously serialised a map (with metadata) with `duratom`, you've lost that metadata. Reading it back with 0.4.2, will result in a  map without metadata, the same as with any other version (the metadata is lost forever). If you then `swap!` with a fn which adds metadata, then that will be custom-printed (with the new custom tag), and read just fine later. As far as I can see existing programs (using the default readers/writers) should not break by upgrading to 0.4.2. Projects that use their own EDN reading/printing, can of course merge their readers (if any) with `duratom.readers/default`. If you have a breaking use-case, please report it asap. It will be much appreciated.  

If you are perfectly content with losing metadata and want to revert to the previous default behaviour, you can do so by overriding the default writer (given your backend). For example, replace `ut/write-edn-object` with `(partial ut/write-edn-object false)` as the default writer, and the newly added metadata support will be completely sidestepped (i.e. the `#duratom/iobj` tag will never be emitted).


##EDN caveats
As explained in [this](https://www.nitor.com/fi/uutiset-ja-blogi/pitfalls-and-bumps-clojures-extensible-data-notation-edn) article, there are some quirks in EDN when used as a serialisation format. Although perfectly valid as general concerns, I see most of them as non-issues for `duratom`. Let me explain...First of all, if your program prints to `*out*`, in a lazy-seq that you're serialising (point 1E), or if you generally you have side-effecting lazy-seqs, you've got bigger problems to worry about. It is commonly understood that you shouldn't do that. Secondly, in my (almost) 10 year Clojure exposure, I've not seen a single project that uses/relies on space-containing keywords (point 1F). In a similar vein, it is extremely rare and rather unidiomatic to put random (typically mutable) Java objects (point 1D) in atoms (or any reference type for that matter). *print-level*, *print-length*, *print-meta* and *print-dup* (point 1B) are explicitly bound to nil by `duratom` prior to printing. Finally, the clojure.edn reader supports namespaced maps (point 1A) as of Clojure 1.9, so if this isn't already considered solved, it will eventually be (as the final consumers eventually update from 1.8). In fact, `duratom` specifies 1.9 as the minimum Clojure version required. That is, strictly speaking, not fully accurate, but if users stick to it, they should be good. Which leaves us with `NaN`(point 1C) - a rather hairy issue from several standpoints, and I'm afraid I don't have an answer here either. Avoid `NaN` to the best of your ability is all I can say. It can/will come back to haunt you in some way, shape or form :(.   


## Requirements

- Java >= 8
- Clojure >= 1.9

### Optional Requirements

- [clojure.java.jdbc](https://github.com/clojure/java.jdbc) >= 0.6.0
    - [org.postgresql/postgresql](https://github.com/pgjdbc/pgjdbc)
    - [org.xerial/sqlite-jdbc](https://github.com/xerial/sqlite-jdbc)
- [amazonica](https://github.com/mcohen01/amazonica)
- [carmine](https://github.com/ptaoussanis/carmine)

## Local development/testing

Tests require PostreSQL and Redis server installed on your machine.
Another option is to use the provided Docker compose configuration in the following way:

1. Install [docker](https://docs.docker.com/install/), [docker-machine](https://docs.docker.com/machine/install-machine/), and [docker-compose](https://docs.docker.com/compose/install/).
2. Create a docker-machine with command `docker-machine create default` (one-off step).
3. Start all databases with command `docker-compose up -d` (or omit the `-d` to spawn the process in the foreground).
4. Now you can run tests freely.
5. When you are done with the development/testing, stop the databases by running `docker-compose down` (or simply `Ctrl-c` once if you didn't use `-d`).

## License

Copyright © 2016 Dimitrios Piliouras

Distributed under the Eclipse Public License, the same as Clojure.
