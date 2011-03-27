{-# LANGUAGE TypeFamilies #-}
module Database.Persist.GenericKVS where
import Control.Monad.IO.Control
import Data.ByteString

class MonadControlIO m => KVSBackend m where
  type CASUnique m
  get :: String -> m (Maybe ByteString)
  gets :: String -> m (Maybe (CASUnique m, ByteString))
  delete, replace, set, add :: String -> ByteString -> m Bool
  cas :: String -> ByteString -> CASUnique m -> m Bool
  add = set
  replace = set

