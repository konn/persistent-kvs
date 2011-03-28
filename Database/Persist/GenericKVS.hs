{-# LANGUAGE TypeFamilies, GeneralizedNewtypeDeriving, ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings, DeriveDataTypeable #-}
module Database.Persist.GenericKVS
    ( KVSBackend (..)
    , KVSPersist (..)
    ) where
import Control.Monad.IO.Control
import Data.ByteString hiding (pack, map, zip, null)
import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.IO.Class
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Int
import Data.Maybe
import Data.Typeable
import Control.Exception.Control
import Prelude hiding (splitAt)
import qualified Database.Persist.Base as P
import Database.Persist.Base hiding (replace, get, delete)
import Database.Persist.KVS.Internal

class MonadControlIO m => KVSBackend m where
  type CASUnique m
  get :: ByteString -> m (Maybe ByteString)
  gets :: ByteString -> m (Maybe (CASUnique m, ByteString))
  delete :: ByteString -> m Bool
  replace, set, add :: ByteString -> ByteString -> m Bool
  cas :: ByteString -> ByteString -> CASUnique m -> m Bool
  add = set
  replace = set

newtype KVSPersist m a = KVSPersist { unKVSPersist :: m a }
  deriving (Functor, Monad, MonadIO, MonadControlIO)

instance MonadTrans KVSPersist where
  lift = KVSPersist

instance (KVSBackend m) => KVSBackend (KVSPersist m) where
  type CASUnique (KVSPersist m) = CASUnique m
  get     = lift . get
  gets    = lift . gets
  delete  = lift . delete
  replace = (lift .) . replace
  set     = (lift .) . set
  add     = (lift .) . add
  cas     = ((lift .) .) . cas

data KVSError = ConstraintError
              deriving (Show, Eq, Typeable)

instance Exception KVSError

pack = T.encodeUtf8 . T.pack

-- Strategy:
-- values/[Entity Name]/[identifier]/[Field Name] : Field Value For Entity
-- identifier/[Entity Name] : Fresh identifier for an Entity
-- uniqs/[Entity Name]/[Unique Name]/[Values] : 
--   Identifier table for an unique name.
--   Values are separated by ":" after url encoded.

instance (KVSBackend m) => PersistBackend (KVSPersist m) where
  insert val = do
    exists <- liftM (not . null . catMaybes) $ mapM get $ map uniqPath $ persistUniqueKeys val
    when exists (throwIO ConstraintError)
    let idPath = intercalate "/" ["identifier", pack $ entityName $ entityDef val]
    mu <- gets idPath
    mident <- case mu of
      Just (uniq, v) -> do
        let ident = fromJust $ fromValue v :: Int64
        success <- cas idPath (toValue $ ident+1) uniq
        if success then return (Just ident) else return Nothing
      Nothing -> set idPath (toValue (0::Int64)) >> return (Just 0)
    case mident of
      Nothing -> insert val
      Just key -> do
        updateColsBy set  key val
        updateUniqueBy set key val
        return $ toPersistKey key

  replace key val = do
    let ident = fromPersistKey key
    old <- liftM fromJust $ P.get key
    updateUniqueBy (const . delete) ident old
    updateUniqueBy set ident val
    updateColsBy replace ident val

  update (key :: Key val) upds = do
    val <- liftM fromJust $ P.get key
    updateUniqueBy (const . delete) (fromPersistKey key) val
    let def = entityDef val
        entName = pack $ entityName def
        cols = map (\(a,b,c) -> a) $ entityColumns def
        udic  = zip (map persistUpdateToFieldName upds) (map persistUpdateToValue upds)
        oldDic = zip cols (map toPersistValue $ toPersistFields val)
        fdic = mapMaybe (\col -> liftM ((,) col) $ lookup col udic `mplus` lookup col oldDic) cols
        ident = (fromPersistKey key)
    a <- either fail return $ fromPersistValues $ map snd fdic
    updateUniqueBy set ident (a :: val)
    forM_ fdic $ \(col, f) -> replace (colPath entName ident $ T.encodeUtf8 $ T.pack col)
                                      (encodePersistValue f)
    return ()

  delete (key :: Key val) = do
    val <- liftM fromJust $ P.get key
    let ident = fromPersistKey key
    updateColsBy (const . delete) ident val
    updateUniqueBy (const . delete) ident val
    return ()
  
  deleteBy uniq = do
    (key, val) <- liftM fromJust $ P.getBy uniq
    let ident = fromPersistKey key
    updateColsBy (const . delete) ident val
    updateUniqueBy (const . delete) ident val
  
  get (key :: Key val) = do
    let def = entityDef (undefined :: val)
        entName = pack $ entityName def
        cols = map (\(a,b,c) -> pack a) $ entityColumns def
    fields <- forM cols $ \colName -> do
      liftM fromJust $ get $ colPath entName (fromPersistKey key) colName
    either (const $ return Nothing) (return . Just) $ fromPersistValues $ mapMaybe decodePersistValue fields

  getBy (uniq :: Unique val) = do
    let entName = pack $ entityName $ entityDef (undefined :: val)
    (mv :: Maybe Int64) <- liftM (fromValue =<<) $ get $ uniqPath uniq
    case mv of
      Nothing -> return Nothing
      Just i  -> do
        let key = toPersistKey i :: Key val
        liftM (liftM ((,) key)) $ P.get key

  updateWhere = undefined
  deleteWhere = undefined
  selectKeys  = undefined
  select = undefined
  count = undefined

updateColsBy :: (KVSBackend m, PersistEntity val)
             => (ByteString -> ByteString -> m a)
             -> Int64 -> val -> m ()
updateColsBy setter key val = do
  let def = entityDef val
      entName = pack $ entityName def
      cols = map (\(a,b,c) -> a) $ entityColumns def
      fdic  = zip cols (toPersistFields val)
  forM_ fdic $ \(col, f) -> setter (colPath entName key $ T.encodeUtf8 $ T.pack col) (toValue f)

updateUniqueBy :: (KVSBackend m, PersistEntity val)
               => (ByteString -> ByteString -> m Bool)
               -> Int64 -> val -> m ()
updateUniqueBy setter key val = do
  let uniqs = persistUniqueKeys val
  forM_ uniqs $ \uniq ->
    setter (uniqPath uniq) (toValue key)
  
colPath :: ByteString -> Int64 -> ByteString -> ByteString
colPath eName key col = intercalate "/" ["values", eName, pack $ show key, col]

uniqPath :: PersistEntity val => Unique val -> ByteString
uniqPath (uniq :: Unique val) =
  let entName = pack $ entityName $ entityDef (undefined :: val)
      vals    = encodeValues $ persistUniqueToValues uniq
      tbls    = encodeValues $ map toPersistValue $ persistUniqueToFieldNames uniq
  in intercalate "/" ["uniqs", entName, tbls, vals]
