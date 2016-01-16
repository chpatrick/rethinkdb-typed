{-# LANGUAGE FlexibleInstances, TypeFamilies, ScopedTypeVariables, FlexibleContexts #-}

module Database.RethinkDB.Typed where

import qualified Database.RethinkDB as R
import Database.RethinkDB (ReQL)
import qualified Database.RethinkDB.Datum as R hiding (Result)

import Data.Boolean
import Data.ByteString (ByteString)
import Data.Coerce
import Data.String
import Data.Text (Text)
import Data.Time

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

newtype Expr a = Expr { unExpr :: ReQL }

type Datum = R.Datum
type Object = R.Object
type Number = Double
type Time = ZonedTime
type String = Text

type instance BooleanOf (Expr a) = Expr Bool

-- Specialize the type of a unary ReQL function.
spec1 :: (ReQL -> ReQL) -> Expr a -> Expr b
spec1 = coerce

-- Specialize the type of a binary ReQL function.
spec2 :: (ReQL -> ReQL -> ReQL) -> Expr a -> Expr b -> Expr c
spec2 = coerce

instance Num (Expr Number) where
  (+) = spec2 (R.+)
  (-) = spec2 (R.-)
  (*) = spec2 (R.*)
  abs x = ifB (x >=* 0) x (-x)
  signum x
    = ifB (x >* 0) 1
        (ifB (x ==* 0) 0 (-1))
  fromInteger = Expr . R.expr

instance Fractional (Expr Number) where
  (/) = spec2 (R./)
  fromRational = Expr . R.expr

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

instance IfB (Expr a) where
  ifB = coerce (R.branch :: ReQL -> ReQL -> ReQL -> ReQL)

-- Given an array, return an array, otherwise return a stream.
type family StreamArray s where
  StreamArray Array = Array
  StreamArray s = Stream

-- Manipulating databases
dbCreate :: Text -> Expr Object
dbCreate = coerce (R.dbCreate :: Text -> ReQL)

dbDrop :: R.Database -> Expr Object
dbDrop = coerce (R.dbDrop :: R.Database -> ReQL)

dbList :: Expr (Array Database.RethinkDB.Typed.String)
dbList = coerce R.dbList

-- Manipulating tables
tableCreate :: R.Table -> Expr Object
tableCreate = coerce (R.tableCreate :: R.Table -> ReQL)

tableDrop :: R.Table -> Expr Object
tableDrop = coerce (R.tableDrop :: R.Table -> ReQL)

tableList :: R.Database -> Expr (Array Database.RethinkDB.Typed.String)
tableList = coerce (R.tableList :: R.Database -> ReQL)

class Changes (s :: * -> *) where
instance Changes SingleSelection
instance Changes Stream

changes :: Changes s => Expr (s a) -> Expr (Stream Object)
changes = spec1 R.changes

-- Writing data

insert :: Expr (Array Object) -> R.Table -> Expr Object
insert = coerce (R.insert :: ReQL -> R.Table -> ReQL)

class Updatable (s :: * -> *) where
instance Updatable Table
instance Updatable Selection
instance Updatable SingleSelection

update :: Updatable s => (Expr Object -> Expr Object) -> Expr (s Object) -> Expr Object
update = coerce (R.update :: (ReQL -> ReQL) -> ReQL -> ReQL)

replace :: Updatable s => (Expr Object -> Expr Object) -> Expr (s Object) -> Expr Object
replace = coerce (R.replace :: (ReQL -> ReQL) -> ReQL -> ReQL)

delete :: Updatable s => Expr (s Object) -> Expr Object
delete = spec1 R.delete

sync :: Expr (Table a) -> Expr Object
sync = spec1 R.sync

-- Selecting data

get :: Expr Database.RethinkDB.Typed.String -> Expr (Table Object) -> Expr (SingleSelection Object)
get = spec2 R.get

table :: Text -> Expr (Table Object)
table = Expr . R.expr . R.table

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
              Expr (s a) -> Expr (s' b) -> Expr (StreamArray s Object)
innerJoin = coerce (R.innerJoin :: (ReQL -> ReQL -> ReQL) -> ReQL -> ReQL -> ReQL)

outerJoin :: (Sequence s, Sequence s') =>
              (Expr a -> Expr b -> Expr Bool) ->
              Expr (s a) -> Expr (s' b) -> Expr (StreamArray s Object)
outerJoin = coerce (R.outerJoin :: (ReQL -> ReQL -> ReQL) -> ReQL -> ReQL -> ReQL)

zip :: Sequence s => Expr (s Object) -> Expr (StreamArray s Object)
zip = spec1 R.zip

-- Transformations

map :: Sequence s => (Expr a -> Expr b) -> Expr (s a) -> Expr (StreamArray s b)
map = coerce (R.map :: (ReQL -> ReQL) -> ReQL -> ReQL)

withFields :: Sequence s => [ Expr Database.RethinkDB.Typed.String ] -> Expr (s Object) -> Expr (StreamArray s Object)
withFields = coerce (R.withFields :: [ ReQL ] -> ReQL -> ReQL)

concatMap :: (Sequence s, Sequence s') => (Expr a -> Expr (s' b)) -> Expr (s a) -> Expr (StreamArray s b)
concatMap = coerce (R.concatMap :: (ReQL -> ReQL) -> ReQL -> ReQL)

skip :: Sequence s => Expr Number -> Expr (s a) -> Expr (StreamArray s a)
skip = spec2 R.skip

limit :: Sequence s => Expr Number -> Expr (s a) -> Expr (StreamArray s a)
limit = spec2 R.limit

isEmpty :: Sequence s => Expr (s a) -> Expr Bool
isEmpty = spec1 R.isEmpty

union :: (Sequence s, Sequence s') => Expr (s a) -> Expr (s' a) -> Expr (StreamArray s a)
union = spec2 R.union

sample :: Sequence s => Expr Number -> Expr (s a) -> Expr (StreamArray s a)
sample = spec2 R.sample

-- Aggregation

reduce :: Sequence s => (Expr a -> Expr a -> Expr a) -> Expr (s a) -> Expr a
reduce = coerce (R.reduce :: (ReQL -> ReQL -> ReQL) -> ReQL -> ReQL)

forEach :: Sequence s => (Expr a -> Expr write) -> Expr (s a) -> Expr Object
forEach = coerce (R.forEach :: (ReQL -> ReQL) -> ReQL -> ReQL)

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

distinct :: Sequence s => Expr (s a) -> Expr (StreamArray s a)
distinct = spec1 R.distinct

contains :: Sequence s => Expr a -> Expr (s a)-> Expr Bool
contains = spec2 R.contains

-- Document manipulation

type family Manip a
type instance Manip (Array Object) = Array Object
type instance Manip Object = Object
type instance Manip (SingleSelection Object) = Object
type instance Manip (Selection Object) = Stream Object
type instance Manip (Stream Object) = Stream Object
type instance Manip (Table Object) = Stream Object

pluck :: [ Expr Database.RethinkDB.Typed.String ] -> Expr a -> Expr (Manip a)
pluck = coerce (R.pluck :: [ ReQL ] -> ReQL -> ReQL)

without :: [ Expr Database.RethinkDB.Typed.String ] -> Expr a -> Expr (Manip a)
without = coerce (R.without :: [ ReQL ] -> ReQL -> ReQL)

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

type family IndexOf a
type instance IndexOf (Array a) = Number
type instance IndexOf Object = Text
type instance IndexOf (SingleSelection Object) = Text
type instance IndexOf (Stream Object) = Text
type instance IndexOf (Selection Object) = Text
type instance IndexOf (Table Object) = Text

type family IndexedOf a
type instance IndexedOf (Array a) = a
type instance IndexedOf Object = Datum
type instance IndexedOf (SingleSelection Object) = Datum
type instance IndexedOf (Stream Object) = Stream Datum
type instance IndexedOf (Selection Object) = Stream Datum
type instance IndexedOf (Table Object) = Stream Datum

index :: Expr (IndexOf a) -> Expr a -> Expr (IndexedOf a)
index = flip $ spec2 (R.!)

class SingleOrObj a where
instance SingleOrObj (SingleSelection Object)
instance SingleOrObj Object

keys :: SingleOrObj o => Expr o -> Expr (Array Database.RethinkDB.Typed.String)
keys = spec1 R.keys

values :: SingleOrObj o => Expr o -> Expr (Array Datum)
values = spec1 R.values

-- String manipulation

match :: Expr Database.RethinkDB.Typed.String -> Expr Database.RethinkDB.Typed.String -> Expr Object
match = spec2 R.match

-- Math and logic
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
  { year :: Expr Number
  , month :: Expr Number
  , day :: Expr Number
  , hour :: Expr Number
  , minute :: Expr Number
  , second :: Expr Number
  , timezone :: Expr Database.RethinkDB.Typed.String
  }

time :: BuildTime -> Expr Time
time bt
  = Expr $ R.time
      (unExpr (year bt))
      (unExpr (month bt))
      (unExpr (day bt))
      (unExpr (hour bt))
      (unExpr (minute bt))
      (unExpr (second bt))
      (unExpr (timezone bt))

-- Assert that this datum is an object.
withObject :: Expr R.Datum -> Expr Object
withObject = coerce

-- Assert that this datum is a number.
withNumber :: Expr R.Datum -> Expr Number
withNumber = coerce

-- Assert that this datum is a string.
withString :: Expr R.Datum -> Expr Database.RethinkDB.Typed.String
withString = coerce

-- Assert that this sequence of datums is a sequence of objects.
withObjects :: Sequence s => Expr (s Datum) -> Expr (s Object)
withObjects = coerce

-- Assert that this sequence of datums is a sequence of numbers.
withNumbers :: Sequence s => Expr (s Datum) -> Expr (s Number)
withNumbers = coerce

type family DatumOf a

type instance DatumOf Datum = Datum
type instance DatumOf Integer = Number
type instance DatumOf Int = Number
type instance DatumOf Double = Number
type instance DatumOf Float = Number
type instance DatumOf [ a ] = Array (DatumOf a)
type instance DatumOf Object = Object

expr :: R.ToDatum a => a -> Expr (DatumOf a)
expr = Expr . R.expr . R.toDatum

type family ResultOf a

type instance ResultOf R.Datum = R.Datum
type instance ResultOf (Table a) = [ ResultOf a ]
type instance ResultOf (Array a) = [ ResultOf a ]
type instance ResultOf (Selection a) = [ ResultOf a ]
type instance ResultOf Number = Number
type instance ResultOf Object = Object
type instance ResultOf Bool = Bool
type instance ResultOf Database.RethinkDB.Typed.String = Database.RethinkDB.Typed.String
type instance ResultOf Time = Time

instance IsString (Expr Database.RethinkDB.Typed.String) where
  fromString = Expr . R.expr

run :: forall a. R.Result (ResultOf a) => R.RethinkDBHandle -> Expr a -> IO (ResultOf a)
run = coerce (R.run :: R.RethinkDBHandle -> ReQL -> IO (ResultOf a))

(&) :: a -> (a -> b) -> b
x & f = f x
infixl 1 &
