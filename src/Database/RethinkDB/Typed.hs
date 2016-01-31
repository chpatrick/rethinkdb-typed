{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.RethinkDB.Typed
  ( -- * The expression type
    Expr(..)
  , DatumOf(..)
  , lit
  , (.=)
  , obj
    -- * Execution
  , ResultOf
  , run
    -- * Primitive types
  , Datum
  , Object
  , Number
  , Time
  , String
  -- * Sequence types
  , Sequence
  , Stream
  , Selection
  , Table
  , Array
  , SingleSelection
  , StreamOrArray
  -- * Manipulating databases
  , dbCreate
  , dbDrop
  , dbList
  -- * Manipulating tables
  , tableCreate
  , tableDrop
  , tableList
  , Observable
  , changes
  -- * Writing data
  , WriteOpts(..)
  , def
  , insert
  , Updatable
  , update
  , replace
  , delete
  , sync
  -- * Selecting data
  , get
  , table
  , Filtered
  , Database.RethinkDB.Typed.filter
  , innerJoin
  , outerJoin
  , left
  , right
  , Database.RethinkDB.Typed.zip
  -- * Transformations
  , Database.RethinkDB.Typed.map
  , withFields
  , Database.RethinkDB.Typed.concatMap
  , skip
  , limit
  , isEmpty
  , union
  , sample
  -- * Aggregation
  , reduce
  , count
  , Database.RethinkDB.Typed.sum
  , avg
  , Database.RethinkDB.Typed.min
  , Database.RethinkDB.Typed.max
  , distinct
  , contains
  -- * Document manipulation
  , Manip
  , pluck
  , without
  , append
  , prepend
  , difference
  , setInsert
  , setUnion
  , setIntersection
  , setDifference
  , IsDatum
  , (!..)
  , SingleOrObj
  , (!)
  , keys
  , values
  -- * Math and logic
  , Database.RethinkDB.Typed.mod
  , random
  , Database.RethinkDB.Typed.round
  , Database.RethinkDB.Typed.floor
  , ceil
  -- * Time and date
  , now
  , BuildTime(..)
  , time
  -- * Control structures
  , forEach
  , range
  , apply
  -- * Type assertions
  , number
  , string
  , bool
  ) where

import qualified Database.RethinkDB as R
import Database.RethinkDB (ReQL)
import Database.RethinkDB.ReQL (Dynamic, Static)
import qualified Database.RethinkDB.Datum as R hiding (Result)

import Data.Boolean
import Data.ByteString (ByteString)
import Data.Coerce
import Data.String (IsString(..))
import Data.Text (Text)
import Data.Time
import Prelude hiding (String)
import qualified Prelude as P

-- | A ReQL expression resulting in type 'a'.
newtype Expr a = Expr { unExpr :: ReQL }

instance R.Expr a => R.Expr (Expr a) where
  expr = unExpr

instance IsString (Expr String) where
  fromString = Expr . R.expr

type family DatumOf a

type instance DatumOf Datum = Datum
type instance DatumOf Integer = Number
type instance DatumOf Int = Number
type instance DatumOf Double = Number
type instance DatumOf Float = Number
type instance DatumOf Rational = Number
type instance DatumOf [ a ] = Array (DatumOf a)
type instance DatumOf Object = Object
type instance DatumOf Text = String
type instance DatumOf String = String
type instance DatumOf Bool = Bool
type instance DatumOf (Expr a) = DatumOf a

-- | Convert a suitable data structure (possibly containing `Expr`s) into an `Expr`.
expr :: R.Expr a => a -> Expr (DatumOf a)
expr = Expr . R.expr

-- | Convert a plain data structure into an `Expr`.
lit :: R.ToDatum a => a -> Expr (DatumOf a)
lit = Expr . R.expr . R.toDatum

(.=) :: IsDatum v => Expr String -> Expr v -> R.Attribute Dynamic
(.=) = coerce ((R.::=) :: ReQL -> ReQL -> R.Attribute Dynamic)

obj :: [ R.Attribute Dynamic ] -> Expr Object
obj = Expr . R.expr

-- Execution

type family ResultOf a

type instance ResultOf R.Datum = R.Datum
type instance ResultOf (Table a) = [ ResultOf a ]
type instance ResultOf (Array a) = [ ResultOf a ]
type instance ResultOf (Selection a) = [ ResultOf a ]
type instance ResultOf Number = Number
type instance ResultOf Object = Object
type instance ResultOf Bool = Bool
type instance ResultOf String = String
type instance ResultOf Time = Time
type instance ResultOf ( a, b ) = Object

run :: forall a. R.Result (ResultOf a) => R.RethinkDBHandle -> Expr a -> IO (ResultOf a)
run = coerce (R.run :: R.RethinkDBHandle -> ReQL -> IO (ResultOf a))

-- Primitives

data Stream a
data Selection a
data Table a
data Array a
data SingleSelection a

class Sequence (s :: * -> *) where

instance Sequence Array where
instance Sequence Stream where
instance Sequence Selection where
instance Sequence Table where

-- | Most ReQL functions can take both sequences and arrays,
-- producing a new array for arrays and a stream otherwise.
--
-- This type family returns the output sequence type for input sequence type 's'.
type family StreamOrArray s where
  StreamOrArray Array = Array
  StreamOrArray s = Stream

type Datum = R.Datum
type Object = R.Object
type Number = Double
type Time = ZonedTime
type String = Text

-- Specialize the type of a unary ReQL function.
spec1 :: (ReQL -> ReQL) -> Expr a -> Expr b
spec1 = coerce

-- Specialize the type of a binary ReQL function.
spec2 :: (ReQL -> ReQL -> ReQL) -> Expr a -> Expr b -> Expr c
spec2 = coerce

-- Manipulating databases
dbCreate :: Text -> Expr Object
dbCreate = coerce (R.dbCreate :: Text -> ReQL)

dbDrop :: R.Database -> Expr Object
dbDrop = coerce (R.dbDrop :: R.Database -> ReQL)

dbList :: Expr (Array String)
dbList = coerce R.dbList

-- Manipulating tables
tableCreate :: R.Table -> Expr Object
tableCreate = coerce (R.tableCreate :: R.Table -> ReQL)

tableDrop :: R.Table -> Expr Object
tableDrop = coerce (R.tableDrop :: R.Table -> ReQL)

tableList :: R.Database -> Expr (Array String)
tableList = coerce (R.tableList :: R.Database -> ReQL)

-- | Sequence types that can be watched for changes.
class Observable (s :: * -> *) where
instance Observable SingleSelection
instance Observable Stream

changes :: Observable s => Expr (s a) -> Expr (Stream Object)
changes = spec1 R.changes

-- Writing data

data WriteOpts = WriteOpts
  { returnChanges :: Bool
  , durability :: Maybe R.Durability
  , nonAtomic :: Bool
  }

def :: WriteOpts
def = WriteOpts False Nothing False

optsToArgs :: WriteOpts -> [ R.Attribute Static ]
optsToArgs opts
  = concat
    [ if returnChanges opts then [ R.returnChanges ] else [] 
    , maybe [] (\d -> [ R.durability d ]) (durability opts)
    , if nonAtomic opts then [ R.nonAtomic ] else []
    ]

insert :: WriteOpts -> Expr (Array Object) -> R.Table -> Expr Object
insert opts
  = coerce ((R.ex R.insert) :: [ R.Attribute Static ] -> ReQL -> R.Table -> ReQL)
      $ optsToArgs opts

-- | Sequence types whose members can be updated.
class Updatable (s :: * -> *) where
instance Updatable Table
instance Updatable Selection
instance Updatable SingleSelection

update :: Updatable s => WriteOpts -> (Expr Object -> Expr Object) -> Expr (s Object) -> Expr Object
update opts
  = coerce ((R.ex R.update) :: [ R.Attribute Static ] -> (ReQL -> ReQL) -> ReQL -> ReQL)
      $ optsToArgs opts

replace :: Updatable s => WriteOpts -> (Expr Object -> Expr Object) -> Expr (s Object) -> Expr Object
replace opts
  = coerce ((R.ex R.replace) :: [ R.Attribute Static ] -> (ReQL -> ReQL) -> ReQL -> ReQL)
      $ optsToArgs opts

delete :: Updatable s => WriteOpts -> Expr (s Object) -> Expr Object
delete opts
  = coerce ((R.ex R.delete) :: [ R.Attribute Static ] -> ReQL -> ReQL)
      $ optsToArgs opts

sync :: Expr (Table a) -> Expr Object
sync = spec1 R.sync

-- Selecting data

get :: Expr String -> Expr (Table Object) -> Expr (SingleSelection Object)
get = spec2 R.get

table :: Text -> Expr (Table Object)
table = Expr . R.expr . R.table

-- | The result sequence type returned by `Database.RethinkDB.Typed.filter` given then input sequence type.
type family Filtered s where
  Filtered Table = Selection
  Filtered Selection = Selection
  Filtered Array = Array
  Filtered Stream = Stream

filter :: Sequence s => (Expr a -> Expr Bool) -> Expr (s a) -> Expr (Filtered s a)
filter = coerce (R.filter :: (ReQL -> ReQL) -> ReQL -> ReQL)

-- Joins

innerJoin :: (Sequence s, Sequence s') =>
              (Expr a -> Expr b -> Expr Bool) ->
              Expr (s a) -> Expr (s' b) -> Expr (StreamOrArray s ( a, b ))
innerJoin = coerce (R.innerJoin :: (ReQL -> ReQL -> ReQL) -> ReQL -> ReQL -> ReQL)

outerJoin :: (Sequence s, Sequence s') =>
              (Expr a -> Expr b -> Expr Bool) ->
              Expr (s a) -> Expr (s' b) -> Expr (StreamOrArray s ( a, b ))
outerJoin = coerce (R.outerJoin :: (ReQL -> ReQL -> ReQL) -> ReQL -> ReQL -> ReQL)

left :: Expr ( a, b ) -> Expr a
left = spec1 (R.! "left")

right :: Expr ( a, b ) -> Expr b
right = spec1 (R.! "right")

zip :: Sequence s => Expr (s ( Object, Object )) -> Expr (StreamOrArray s Object)
zip = spec1 R.zip

-- Transformations

map :: Sequence s => (Expr a -> Expr b) -> Expr (s a) -> Expr (StreamOrArray s b)
map = coerce (R.map :: (ReQL -> ReQL) -> ReQL -> ReQL)

withFields :: Sequence s => [ Expr String ] -> Expr (s Object) -> Expr (StreamOrArray s Object)
withFields = coerce (R.withFields :: [ ReQL ] -> ReQL -> ReQL)

concatMap :: (Sequence s, Sequence s') => (Expr a -> Expr (s' b)) -> Expr (s a) -> Expr (StreamOrArray s b)
concatMap = coerce (R.concatMap :: (ReQL -> ReQL) -> ReQL -> ReQL)

skip :: Sequence s => Expr Number -> Expr (s a) -> Expr (StreamOrArray s a)
skip = spec2 R.skip

limit :: Sequence s => Expr Number -> Expr (s a) -> Expr (StreamOrArray s a)
limit = spec2 R.limit

isEmpty :: Sequence s => Expr (s a) -> Expr Bool
isEmpty = spec1 R.isEmpty

union :: (Sequence s, Sequence s') => Expr (s a) -> Expr (s' a) -> Expr (StreamOrArray s a)
union = spec2 R.union

instance Monoid (Expr (Array a)) where
  mempty = Expr $ R.expr ([] :: [ Datum ])
  mappend = union

sample :: Sequence s => Expr Number -> Expr (s a) -> Expr (StreamOrArray s a)
sample = spec2 R.sample

-- Aggregation

reduce :: Sequence s => (Expr a -> Expr a -> Expr a) -> Expr (s a) -> Expr a
reduce = coerce (R.reduce :: (ReQL -> ReQL -> ReQL) -> ReQL -> ReQL)

count :: Sequence s => Expr (s a) -> Expr Number
count = spec1 R.count

sum :: Sequence s => Expr (s Number) -> Expr Number
sum = spec1 R.sum

avg :: Sequence s => Expr (s Number) -> Expr Number
avg = spec1 R.avg

min :: Sequence s => Expr (s a) -> Expr a
min = spec1 R.min

max :: Sequence s => Expr (s a) -> Expr a
max = spec1 R.max

distinct :: Sequence s => Expr (s a) -> Expr (StreamOrArray s a)
distinct = spec1 R.distinct

contains :: Sequence s => Expr a -> Expr (s a) -> Expr Bool
contains = spec2 R.contains

-- Document manipulation

-- | The result of applying `pluck` or `without` to an expression.
type family Manip a
type instance Manip (Array Object) = Array Object
type instance Manip Object = Object
type instance Manip (SingleSelection Object) = Object
type instance Manip (Selection Object) = Stream Object
type instance Manip (Stream Object) = Stream Object
type instance Manip (Table Object) = Stream Object

pluck :: [ Expr String ] -> Expr a -> Expr (Manip a)
pluck = coerce (R.pluck :: [ ReQL ] -> ReQL -> ReQL)

without :: [ Expr String ] -> Expr a -> Expr (Manip a)
without = coerce (R.without :: [ ReQL ] -> ReQL -> ReQL)

merge :: Expr Object -> Expr Object -> Expr Object
merge = spec2 R.merge

append :: Expr a -> Expr (Array a) -> Expr (Array a)
append = spec2 R.append

prepend :: Expr a -> Expr (Array a) -> Expr (Array a)
prepend = spec2 R.prepend

difference :: Expr (Array a) -> Expr (Array a) -> Expr (Array a)
difference = spec2 R.difference

setInsert :: Expr a -> Expr (Array a) -> Expr (Array a)
setInsert = spec2 R.setInsert

setUnion :: Expr (Array a) -> Expr (Array a) -> Expr (Array a)
setUnion = spec2 R.setUnion

setIntersection :: Expr (Array a) -> Expr (Array a) -> Expr (Array a)
setIntersection = spec2 R.setIntersection

setDifference :: Expr (Array a) -> Expr (Array a) -> Expr (Array a)
setDifference = spec2 R.setDifference

class IsDatum a where

instance IsDatum Datum
instance IsDatum Bool
instance IsDatum Number
instance IsDatum Object
instance IsDatum Time
instance IsDatum String
instance IsDatum a => IsDatum (Array a)

class SingleOrObj a where
instance SingleOrObj (SingleSelection Object)
instance SingleOrObj Object

-- | Index an `Array`.
(!..) :: Expr (Array a) -> Expr Number -> Expr a
(!..) = spec2 (R.!)

-- | Index an object.
(!) :: (SingleOrObj o, IsDatum a) => Expr o -> Expr Text -> Expr a
(!) = spec2 (R.!)

keys :: SingleOrObj o => Expr o -> Expr (Array String)
keys = spec1 R.keys

values :: SingleOrObj o => Expr o -> Expr (Array Datum)
values = spec1 R.values

-- String manipulation

match :: Expr String -> Expr String -> Expr Object
match = spec2 R.match

-- Math and logic
instance Num (Expr Number) where
  (+) = spec2 (R.+)
  (-) = spec2 (R.-)
  (*) = spec2 (R.*)
  abs x = ifB (x >=* 0) x (-x)
  signum x
    = ifB (x >* 0) 1
        (ifB (x ==* 0) 0 (-1))
  fromInteger = lit

instance Fractional (Expr Number) where
  (/) = spec2 (R./)
  fromRational = lit

mod :: Expr Number -> Expr Number -> Expr Number
mod = spec2 R.mod

type instance BooleanOf (Expr a) = Expr Bool

instance Boolean (Expr Bool) where
  true = Expr $ R.expr True
  false = Expr $ R.expr False
  notB = spec1 R.not
  (&&*) = spec2 (R.&&)
  (||*) = spec2 (R.||)

instance EqB (Expr a) where
  (==*) = spec2 (R.==)

instance OrdB (Expr a) where
  (<*) = spec2 (R.<)
  (>=*) = spec2 (R.>=)
  (>*) = spec2 (R.>)
  (<=*) = spec2 (R.<=)

random :: Expr Number
random = coerce R.random

round :: Expr Number -> Expr Number
round = spec1 R.round

floor :: Expr Number -> Expr Number
floor = spec1 R.floor

ceil :: Expr Number -> Expr Number
ceil = spec1 R.ceil

-- Dates and times

now :: Expr Time
now = coerce R.now

data BuildTime = BuildTime
  { yearPart :: Expr Number
  , monthPart :: Expr Number
  , dayPart :: Expr Number
  , hourPart :: Expr Number
  , minutePart :: Expr Number
  , secondPart :: Expr Number
  , timezonePart :: Expr String
  }

time :: BuildTime -> Expr Time
time bt
  = coerce R.time
      (yearPart bt)
      (monthPart bt)
      (dayPart bt)
      (hourPart bt)
      (minutePart bt)
      (secondPart bt)
      (timezonePart bt)

-- Control structures

instance IfB (Expr a) where
  ifB = coerce (R.branch :: ReQL -> ReQL -> ReQL -> ReQL)

forEach :: Sequence s => (Expr a -> Expr write) -> Expr (s a) -> Expr Object
forEach = coerce (R.forEach :: (ReQL -> ReQL) -> ReQL -> ReQL)

range :: Expr Number -> Expr (Stream Number)
range = spec1 R.range

apply :: (Expr a -> Expr b) -> Expr a -> Expr b
apply f x = coerce (R.apply :: ((ReQL -> ReQL) -> [ ReQL ] -> ReQL)) f [x]

-- Type assertions (useful to reduce ambiguity)
number :: Expr Number -> Expr Number
number = id

string :: Expr String -> Expr String
string = id

bool :: Expr Bool -> Expr Bool
bool = id
