{-# LANGUAGE GeneralizedNewtypeDeriving, FlexibleContexts, TypeFamilies, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables  #-}
module Database.Persist.Memcached where
import Data.ByteString hiding (take, map, zip, pack)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Database.Persist.Base
import Data.Int
import Prelude hiding (take, foldr, length)

import Control.Monad
import Data.Maybe

import Database.Persist
import qualified Database.Persist.GenericKVS as KVS
import Database.Persist.KVS.Internal
import Data.Int

pack = T.encodeUtf8 . T.pack

-- Strategy:
-- values/[Entity Name]/[identifier]/[Field Name] : Field Value For Entity
-- identifier/[Entity Name] : Fresh identifier for an Entity
-- uniqs/[Entity Name]/[Unique Name]/[Values] : 
--   Identifier table for an unique name.
--   Values are separated by ":" after url encoded.

instance (KVS.KVSBackend m) => PersistBackend (KVS.KVSPersist m) where
  insert val = do
    let def = entityDef val
        entName = pack $ entityName def
        idPath = intercalate "/" ["identifier", pack $ entityName def]
        cols = map (\(a,b,c) -> a) $ entityColumns def
        fdic  = zip cols (toPersistFields val)
    mu <- KVS.gets idPath
    mident <- case mu of
      Just (uniq, v) -> do
        let ident = fromJust $ fromValue v :: Int64
        success <- KVS.cas idPath (toValue $ ident+1) uniq
        if success then return (Just ident) else return Nothing
      Nothing -> KVS.set idPath (toValue (0::Int64)) >> return (Just 0)
    case mident of
      Nothing -> insert val
      Just key -> do
        forM fdic $ \(col, f) -> KVS.set (colPath entName key $ T.encodeUtf8 $ T.pack col) (toValue f)
        updateUniqueBy KVS.set key val
        return $ toPersistKey key

  replace key val = do
    let def = entityDef val
        entName = pack $ entityName def
        cols = map (\(a,b,c) -> a) $ entityColumns def
        fdic  = zip cols (toPersistFields val)
        ident = (fromPersistKey key)
    updateUniqueBy KVS.replace ident val
    forM_ fdic $ \(col, f) -> KVS.replace (colPath entName ident $ T.encodeUtf8 $ T.pack col) (toValue f)

  update (key :: Key val) upds = do
    val <- liftM fromJust $ get key
    let def = entityDef val
        entName = pack $ entityName def
        cols = map (\(a,b,c) -> a) $ entityColumns def
        udic  = zip (map persistUpdateToFieldName upds) (map persistUpdateToValue upds)
        oldDic = zip cols (map toPersistValue $ toPersistFields val)
        fdic = mapMaybe (\col -> liftM ((,) col) $ lookup col udic `mplus` lookup col oldDic) cols
        ident = (fromPersistKey key)
    a <- either fail return $ fromPersistValues $ map snd fdic
    updateUniqueBy KVS.replace ident (a :: val)
    forM_ fdic $ \(col, f) -> KVS.replace (colPath entName ident $ T.encodeUtf8 $ T.pack col)
                                          (encodePersistValue f)
    return ()

updateUniqueBy :: (KVS.KVSBackend m, PersistEntity val)
               => (ByteString -> ByteString -> m Bool)
               -> Int64 -> val -> m Bool
updateUniqueBy setter key val = do
  let def = entityDef val
      entName = pack $ entityName def
      cols = map (\(a,b,c) -> a) $ entityColumns def
      uniqs = entityUniques def
      fdic  = zip cols (toPersistFields val)
  forM uniqs $ \(fname, keys) -> do
    let val = intercalate ":" $ map (toValue . fromJust . flip lookup fdic) keys
        uniqPath = intercalate "/" ["uniqs", entName, pack fname]
    setter uniqPath val
  return True
  
colPath :: ByteString -> Int64 -> ByteString -> ByteString
colPath eName key col = intercalate "/" ["values", eName, pack $ show key, col]

