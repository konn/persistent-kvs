{-# LANGUAGE TypeFamilies #-}
module Database.Persist.GenericKVS where
import Control.Monad.IO.Control
import Data.ByteString

class MonadControlIO m => KVSBackend m where
  type CASUnique m
  get :: ByteString -> m (Maybe ByteString)
  gets :: ByteString -> m (Maybe (CASUnique m, ByteString))
  delete, replace, set, add :: ByteString -> ByteString -> m Bool
  cas :: ByteString -> ByteString -> CASUnique m -> m Bool
  add = set
  replace = set

